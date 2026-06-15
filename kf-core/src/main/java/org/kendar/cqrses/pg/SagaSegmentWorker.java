package org.kendar.cqrses.pg;

import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.observability.Observability;
import org.kendar.cqrses.repositories.CheckpointStore;
import org.kendar.cqrses.repositories.EventStore;
import org.kendar.cqrses.scheduler.Sleeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The saga half of the cluster pull pump, for one OWNED segment {@code s}. A saga
 * in {@code segment(sagaId) == s} can correlate events emitted by aggregates in
 * <i>any</i> segment, so this worker merges all {@code SEGMENTS} source streams:
 * <ol>
 *   <li>reads each source segment {@code k} via
 *       {@link EventStore#loadSegmentTypeTail} (gap-free, exactly ordered by
 *       {@code segment_seq}, filtered to the group's subscribed types),</li>
 *   <li>merges by {@code createdAt} with a low-watermark (an idle/empty source
 *       contributes {@code now − clockSkew} so it cannot stall the merge),</li>
 *   <li>drives {@link SagaResolver#resolveForSegment} per merged event — the
 *       create/update split, scoped to {@code s},</li>
 *   <li>advances the per-source checkpoint {@code (group, s, k)} as each source is
 *       consumed.</li>
 * </ol>
 * This is the only place the "approximate global" ordering shows up; it matches the
 * framework's no-cross-aggregate-ordering stance. See
 * {@code plans/kf-cluster-itemprocessor.md} §2d and {@code docs/tricks.md}.
 *
 * <p><b>Checkpoints stay per-event here</b> (unlike the projection worker's
 * per-batch save): replaying a saga re-fires its side effects (re-emitted
 * commands), so the crash-replay window is deliberately kept at one event —
 * widening it to a whole batch would multiply the re-fired commands the
 * downstream OCC has to arbitrate. Saga volume is low; the saved UPSERTs would
 * not be worth that.
 *
 * <p>Idle waiting is a {@link Semaphore}-parked backstop poll (see
 * {@link PumpNudger}): a local append nudges the worker awake immediately;
 * cross-node events are picked up within {@link SegmentProcessor#BACKSTOP_MS}.
 */
class SagaSegmentWorker {

    private static final Logger LOGGER = LoggerFactory.getLogger(SagaSegmentWorker.class);
    /**
     * Cross-node NTP-drift budget for the empty-source watermark (ms). Saga event
     * ordering across aggregates/segments is <b>best-effort by {@code createdAt}</b>
     * within this window — in-order per correlated aggregate, approximate across
     * (grill item 8). A strict global order would need a cluster-wide append
     * serialization point, the bottleneck the per-segment counter avoids; NTP
     * discipline (drift &lt; skew) is the stated deployment requirement.
     *
     * <p>Tunable via the {@code kf.saga.clockSkewMs} system property (default 200) or
     * {@link #setClockSkewMs(long)}; replaces the former buried literal.
     */
    private static volatile long clockSkewMs = Long.getLong("kf.saga.clockSkewMs", 200L);

    /** Override the cross-node clock-skew budget (ms) used by the saga merge watermark. */
    static void setClockSkewMs(long ms) {
        clockSkewMs = ms;
    }

    /** The current cross-node clock-skew budget (ms). */
    static long getClockSkewMs() {
        return clockSkewMs;
    }
    /** Back-off between retries when a store/checkpoint call fails transiently. */
    private static final long ERROR_BACKOFF_MS = 500L;

    private final ProcessingGroupsManager eventManager;
    private final EventStore eventStore;
    private final CheckpointStore checkpointStore;
    private final String group;
    private final int ownedSegment;
    private final int segments;
    private final int batchLimit;
    private final AtomicBoolean running;
    /** Coalescing wakeup released by a local append (via {@link PumpNudger}). */
    private final Semaphore wakeup = new Semaphore(0);
    private final Runnable nudgeRef = this::nudge;

    SagaSegmentWorker(ProcessingGroupsManager eventManager, EventStore eventStore,
                      CheckpointStore checkpointStore, String group, int ownedSegment,
                      int segments, int batchLimit, AtomicBoolean running) {
        this.eventManager = eventManager;
        this.eventStore = eventStore;
        this.checkpointStore = checkpointStore;
        this.group = group;
        this.ownedSegment = ownedSegment;
        this.segments = segments;
        this.batchLimit = batchLimit;
        this.running = running;
    }

    /** Cheap, never throws — safe to call from any publishing thread. */
    private void nudge() {
        if (wakeup.availablePermits() == 0) {
            wakeup.release();
        }
    }

    /**
     * Park until a local append nudges us or {@code maxMs} elapses, then drain
     * leftover permits so coalesced nudges trigger exactly one re-evaluation.
     * Returns {@code false} when interrupted (caller should exit).
     */
    private boolean park(long maxMs) {
        try {
            wakeup.tryAcquire(maxMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
        }
        wakeup.drainPermits();
        return true;
    }

    void run() {
        SagaResolver resolver = eventManager.resolver(group);
        if (resolver == null) return;
        Set<String> types = eventManager.subscribedEventTypes(group);
        if (types.isEmpty()) return;

        PumpNudger.register(nudgeRef);
        try {
            mergeLoop(resolver, types);
        } finally {
            PumpNudger.unregister(nudgeRef);
        }
    }

    private void mergeLoop(SagaResolver resolver, Set<String> types) {

        // Per-source cursor + buffer (refilled when drained).
        long[] cp = new long[segments];
        List<List<InternalMessage>> buf = new ArrayList<>(segments);
        int[] idx = new int[segments];
        for (int k = 0; k < segments; k++) {
            cp[k] = checkpointStore.load(group, ownedSegment, k);
            buf.add(List.of());
        }

        while (running.get()) {
            try {
                refillEmptyBuffers(types, cp, buf, idx);

                // candidate = the buffered head with the smallest createdAt (tie → lowest k).
                int pick = -1;
                long bestTs = Long.MAX_VALUE;
                for (int k = 0; k < segments; k++) {
                    if (idx[k] < buf.get(k).size()) {
                        long ts = buf.get(k).get(idx[k]).getCreatedAt();
                        if (ts < bestTs) {
                            bestTs = ts;
                            pick = k;
                        }
                    }
                }
                if (pick < 0) {
                    // Every source drained — park until a local append nudges us or
                    // the backstop expires (cross-node events need the timed poll).
                    if (!park(SegmentProcessor.BACKSTOP_MS)) return;
                    continue;
                }

                // Low-watermark: min over sources of (head createdAt | now-skew if empty).
                long now = Instant.now().toEpochMilli();
                long watermark = Long.MAX_VALUE;
                for (int k = 0; k < segments; k++) {
                    long wmk = (idx[k] < buf.get(k).size())
                            ? buf.get(k).get(idx[k]).getCreatedAt()
                            : (now - clockSkewMs);
                    watermark = Math.min(watermark, wmk);
                }
                if (bestTs > watermark) {
                    // A lagging/empty source might still produce an earlier event — wait
                    // until the candidate ages past the watermark. The watermark advances
                    // with wall time, so the needed wait is computable; a nudge (possible
                    // earlier event from a local append) re-evaluates sooner.
                    long waitMs = Math.clamp(bestTs + clockSkewMs - now, 1L, 50L);
                    if (!park(waitMs)) return;
                    continue;
                }

                InternalMessage m = buf.get(pick).get(idx[pick]);
                try {
                    resolver.resolveForSegment(m, ownedSegment);
                } catch (Exception e) {
                    // Mirror the resolver-thread contract: one bad event must not kill
                    // the worker (which would stall this group's segment).
                    LOGGER.error("saga resolve error group={} seg={} sourceSeg={}",
                            group, ownedSegment, pick, e);
                }
                idx[pick]++;
                cp[pick] = m.getSegmentSeq();
                checkpointStore.save(group, ownedSegment, pick, cp[pick]);
                Observability.get().onCheckpointSaved(group, ownedSegment);
            } catch (Exception e) {
                // A transient store/checkpoint failure (loadSegmentTypeTail / save) must
                // not kill the saga lane: the cluster keeps the segment parked-and-owned,
                // so a dead worker silently stalls this group's segment forever. Back off
                // and retry from the last durable per-source checkpoint. The resolver's own
                // errors are isolated above, so this only catches pump-loop failures.
                LOGGER.warn("saga worker group={} seg={} transient error, retrying: {}",
                        group, ownedSegment, e.getMessage());
                Sleeper.sleep(ERROR_BACKOFF_MS);
            }
        }
    }

    private void refillEmptyBuffers(Set<String> types, long[] cp,
                                    List<List<InternalMessage>> buf, int[] idx) {
        for (int k = 0; k < segments; k++) {
            if (idx[k] >= buf.get(k).size()) {
                long t0 = System.nanoTime();
                List<InternalMessage> rows = eventStore.loadSegmentTypeTail(k, types, cp[k], batchLimit);
                Observability.get().onSegmentTailRead(group, rows.size(), System.nanoTime() - t0);
                buf.set(k, rows);
                idx[k] = 0;
            }
        }
    }
}
