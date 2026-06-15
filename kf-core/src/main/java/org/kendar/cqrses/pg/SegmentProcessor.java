package org.kendar.cqrses.pg;

import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.cluster.spi.SegmentOwnership;
import org.kendar.cqrses.observability.Observability;
import org.kendar.cqrses.repositories.CheckpointStore;
import org.kendar.cqrses.repositories.EventStore;
import org.kendar.cqrses.scheduler.Sleeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The real cluster event-side processor: the durable segment-pull bridge. The node
 * owns a <b>dynamic set of segments</b> ({@link #ownedSegments}); the leader grows
 * and shrinks it through {@link #claimSegment}/{@link #releaseSegment}. The
 * event-side work runs on <b>one projection worker thread per processing group</b>
 * (optionally fanned out into {@code dispatchConcurrency} segment-partitioned
 * slots), <b>not</b> one thread per {@code (group, segment)} — each worker polls the
 * {@link EventStore} for <i>all</i> the segments this node owns in a single wide
 * read ({@link EventStore#loadSegmentsTail}), dispatches each event through the
 * (pull-mode, thread-less) {@link ProcessingGroupsManager} lanes, and advances a
 * durable {@link CheckpointStore} high-water-mark <b>after</b> successful dispatch
 * (at-least-once; projections must be idempotent under replay).
 * <p>
 * <b>Ownership is the live {@code ownedSegments} set, gating dispatch.</b> For every
 * polled event the worker recomputes its segment from the aggregate id
 * ({@link SegmentCalculator#calculateSegment}) and <b>skips</b> it when the segment
 * is no longer owned. In-flight events run to completion — the skip gates
 * <i>starting</i> a dispatch, never aborts a running handler. This is what makes
 * {@link #releaseSegment} a simple "remove from the set, let any in-flight dispatch
 * finish, then clear the lease" rather than a per-{@code (group,segment)} thread
 * join, and it preserves per-aggregate ordering (one aggregate → one segment → one
 * owner → one worker thread). See {@code docs/tricks.md} and
 * {@code plans/singlePgForNode.md}.
 * <p>
 * <b>Sagas are unchanged this round:</b> a saga group still runs one
 * {@link SagaSegmentWorker} per owned {@code (group, segment)} (it k-way-merges all
 * {@code SEGMENTS} source streams per owned target segment, which does not collapse
 * into a single wide read). See {@code plans/singlePgForNode.md} §3.
 */
public class SegmentProcessor implements SegmentOwnership {

    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentProcessor.class);
    public static final int DEFAULT_BATCH = 256;
    /** Per-worker join budget on release. */
    private static final long JOIN_TIMEOUT_MS = 10_000L;
    /**
     * Back-off between retries when a worker's own store/checkpoint call fails
     * transiently (lock-wait timeout, deadlock, dropped connection). Long enough
     * to let the contention pass, short enough that convergence still catches up.
     */
    private static final long ERROR_BACKOFF_MS = 500L;
    /**
     * Idle park between polls when a tail read came back empty. Locally-appended
     * events cut this short via {@link PumpNudger} (~zero same-node latency); the
     * timed poll is only the backstop for events appended by <i>other</i> nodes,
     * which cannot nudge this JVM. Bounds idle DB load to ~10 tail reads/s per
     * worker instead of the previous spin-loop's unbounded re-query.
     */
    static final long BACKSTOP_MS = 100L;

    private final ProcessingGroupsManager eventManager;
    private final EventStore eventStore;
    private final CheckpointStore checkpointStore;
    private final int batchLimit;
    private final int segments;
    private final int dispatchConcurrency;

    /** The live set of segments this node owns — the authority gating dispatch. */
    private final Set<Integer> ownedSegments = ConcurrentHashMap.newKeySet();
    /** One projection worker per {@code group#slot} — long-lived, started once. */
    private final Map<String, ProjectionGroupWorker> projectionWorkers = new ConcurrentHashMap<>();
    /** One saga worker per {@code group#segment} — started on claim, stopped on release. */
    private final Map<String, SagaWorker> sagaWorkers = new ConcurrentHashMap<>();
    /** Per-segment park latch — the cluster pump thread blocks here while it owns the segment. */
    private final Map<Integer, CountDownLatch> parkLatches = new ConcurrentHashMap<>();

    public SegmentProcessor(ProcessingGroupsManager eventManager, EventStore eventStore,
                            CheckpointStore checkpointStore) {
        this(eventManager, eventStore, checkpointStore, DEFAULT_BATCH, 1);
    }

    public SegmentProcessor(ProcessingGroupsManager eventManager, EventStore eventStore,
                            CheckpointStore checkpointStore, int batchLimit) {
        this(eventManager, eventStore, checkpointStore, batchLimit, 1);
    }

    /**
     * @param dispatchConcurrency projection dispatch threads per group. {@code 1}
     *                            (default) means strictly one thread per group; a
     *                            higher value fans dispatch out across that many
     *                            segment-partitioned slots ({@code segment %
     *                            dispatchConcurrency}) so per-aggregate ordering still
     *                            holds (a segment always maps to the same slot).
     */
    public SegmentProcessor(ProcessingGroupsManager eventManager, EventStore eventStore,
                            CheckpointStore checkpointStore, int batchLimit, int dispatchConcurrency) {
        this.eventManager = eventManager;
        this.eventStore = eventStore;
        this.checkpointStore = checkpointStore;
        this.batchLimit = batchLimit;
        this.segments = SegmentCalculator.getSegments();
        this.dispatchConcurrency = Math.max(1, dispatchConcurrency);
    }

    private static String key(String group, int n) {
        return group + "#" + n;
    }

    @Override
    public void claimSegment(int segment) {
        CountDownLatch park = new CountDownLatch(1);
        // If a stale latch exists (re-claim), release it so the old parker exits.
        CountDownLatch prev = parkLatches.put(segment, park);
        if (prev != null) prev.countDown();

        ownedSegments.add(segment);
        ensureProjectionWorkers();
        // Sagas keep one worker per owned (group, segment) — see class javadoc.
        for (String group : eventManager.sagaGroups()) {
            startSagaWorker(group, segment);
        }
        LOGGER.info("claimed segment {} ({} owned, {} projection workers, {} saga workers)",
                segment, ownedSegments.size(), projectionWorkers.size(), sagaWorkers.size());
        // Wake the (possibly parked) workers so the new segment's backlog is
        // picked up immediately instead of after the backstop interval.
        for (ProjectionGroupWorker w : projectionWorkers.values()) {
            w.nudge();
        }
        try {
            park.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Start the per-group(-slot) projection workers once; idempotent across claims. */
    private void ensureProjectionWorkers() {
        for (String group : eventManager.projectionGroups()) {
            for (int slot = 0; slot < dispatchConcurrency; slot++) {
                String k = key(group, slot);
                ProjectionGroupWorker w = new ProjectionGroupWorker(group, slot);
                if (projectionWorkers.putIfAbsent(k, w) == null) {
                    w.startThread();
                }
            }
        }
    }

    private void startSagaWorker(String group, int segment) {
        SagaWorker w = new SagaWorker(group, segment);
        if (sagaWorkers.putIfAbsent(key(group, segment), w) == null) {
            w.startThread();
        }
    }

    @Override
    public void releaseSegment(int segment, Runnable onDrained) {
        Thread drainer = new Thread(() -> drain(segment, onDrained), "seg-release-" + segment);
        drainer.setDaemon(true);
        drainer.start();
    }

    private void drain(int segment, Runnable onDrained) {
        LOGGER.info("releasing segment {} (was {} owned: {})",
                segment, ownedSegments.size(), ownedSegments);
        // 1. Remove from the owned set FIRST: every subsequent projection dispatch
        //    for this segment now skips, and the next wide read drops it.
        ownedSegments.remove(segment);
        // 2. Barrier each projection worker: acquiring its gate waits out any batch it
        //    is mid-dispatch (in-flight events for this segment finish and checkpoint),
        //    and once we hold-then-release the gate the worker cannot start a fresh
        //    dispatch for the now-unowned segment. No-double-pump: the gaining node's
        //    claim can only proceed after onDrained clears the lease, which is below.
        for (ProjectionGroupWorker w : projectionWorkers.values()) {
            w.barrier();
        }
        // 3. Stop this segment's saga workers (still per-(group, segment)).
        for (String group : eventManager.sagaGroups()) {
            SagaWorker w = sagaWorkers.remove(key(group, segment));
            if (w != null) {
                w.running.set(false);
                join(w.thread);
            }
        }
        try {
            if (onDrained != null) onDrained.run();
            LOGGER.info("released segment {} (lease cleared, {} still owned: {})",
                    segment, ownedSegments.size(), ownedSegments);
        } catch (RuntimeException e) {
            LOGGER.warn("onDrained for segment {} threw: {}", segment, e.getMessage());
        } finally {
            CountDownLatch park = parkLatches.remove(segment);
            if (park != null) park.countDown(); // unblock the parked claimSegment
        }
    }

    /**
     * Operator-initiated rebuild from history. For each target group this node owns
     * a segment of: rewind its {@code processor_checkpoint} row(s) to re-tail from
     * {@code fromSeq} and let the worker pick the reset cursor up. {@code fromSeq ==
     * 0} ⇒ full rebuild. Idempotent projections make this safe and side-effect-free.
     * <p>
     * <b>Saga groups are excluded unless {@code includeSagas}</b>: re-driving a saga
     * re-fires its side effects (re-emitted commands). The replay is naturally
     * partitioned by segment — each node replays only the segments it currently owns.
     * See {@code plans/singlePgForNode.md} §3 and {@code kf-cluster-itemprocessor.md} §2f.
     */
    public void replay(Set<String> groups, long fromSeq, boolean includeSagas) {
        // afterSeq cursor is strictly-greater, so re-running every event >= fromSeq
        // means seeding the checkpoint to fromSeq-1.
        long checkpoint = fromSeq - 1;
        Set<Integer> owned = new HashSet<>(ownedSegments);
        for (String group : groups) {
            boolean isSaga = eventManager.sagaGroups().contains(group);
            boolean isProjection = eventManager.projectionGroups().contains(group);
            if (!isSaga && !isProjection) {
                continue; // group not dispatched on this node
            }
            if (isSaga) {
                if (!includeSagas) {
                    LOGGER.info("replay: skipping saga group {} (includeSagas=false)", group);
                    continue;
                }
                replaySaga(group, owned, checkpoint, fromSeq);
            } else {
                // Projection: reset the owned segments' checkpoints under the worker's
                // gate (so it cannot be mid-dispatch) and drop the in-memory cursor so
                // the next wide read re-tails from the reset value.
                for (int slot = 0; slot < dispatchConcurrency; slot++) {
                    ProjectionGroupWorker w = projectionWorkers.get(key(group, slot));
                    if (w != null) w.reset(owned, checkpoint);
                }
                LOGGER.info("replay: projection group={} segs={} reset to fromSeq={}", group, owned, fromSeq);
            }
        }
    }

    private void replaySaga(String group, Set<Integer> owned, long checkpoint, long fromSeq) {
        for (int seg : owned) {
            SagaWorker old = sagaWorkers.remove(key(group, seg));
            if (old != null) {
                old.running.set(false);
                join(old.thread);
            }
            // reset(), not save(): replay deliberately rewinds the cursor to re-tail
            // from history; the monotonic save() would refuse the lowering. A saga
            // group keeps SEGMENTS per-source checkpoints per owned segment.
            for (int k = 0; k < segments; k++) {
                checkpointStore.reset(group, seg, k, checkpoint);
            }
            startSagaWorker(group, seg);
            LOGGER.info("replay: saga group={} seg={} reset to fromSeq={}", group, seg, fromSeq);
        }
    }

    /** Stop every worker (best effort) — for {@code Framework.stop}/test teardown. */
    public void stopAll() {
        for (ProjectionGroupWorker w : projectionWorkers.values()) {
            w.running.set(false);
            w.nudge(); // unpark a worker waiting on its backstop so it exits promptly
            PumpNudger.unregister(w.nudgeRef);
        }
        for (SagaWorker w : sagaWorkers.values()) {
            w.running.set(false);
        }
        for (CountDownLatch park : parkLatches.values()) {
            park.countDown();
        }
        parkLatches.clear();
        projectionWorkers.clear();
        sagaWorkers.clear();
        ownedSegments.clear();
    }

    private static void join(Thread t) {
        if (t == null) return;
        try {
            t.join(JOIN_TIMEOUT_MS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    // ---- projection worker: one thread per (group, slot) over owned segments ----

    private final class ProjectionGroupWorker {
        final String group;
        final int slot;
        final AtomicBoolean running = new AtomicBoolean(true);
        /** Per-owned-segment tail cursor (last dispatched segment_seq). */
        final Map<Integer, Long> cursor = new HashMap<>();
        /**
         * Serialises a whole poll-batch against {@link #barrier()} and {@link #reset}:
         * holding it means "this worker is mid-batch", so a releasing/replaying caller
         * waits for in-flight dispatch to finish before it observes a quiesced worker.
         */
        final Object gate = new Object();
        /**
         * Coalescing wakeup: a local append releases at most one permit (a benign
         * over-release while draining costs one extra empty poll), so N rapid
         * appends collapse into a single drain instead of N polls.
         */
        final Semaphore wakeup = new Semaphore(0);
        /** Stable listener reference so register/unregister pair up. */
        private final Runnable nudgeRef = this::nudge;
        Thread thread;
        /** Throttle for the alive-heartbeat log (epoch ms of last emit). */
        private long lastHeartbeatMs;

        ProjectionGroupWorker(String group, int slot) {
            this.group = group;
            this.slot = slot;
        }

        /** Cheap, never throws — safe to call from any publishing thread. */
        void nudge() {
            if (wakeup.availablePermits() == 0) {
                wakeup.release();
            }
        }

        void startThread() {
            PumpNudger.register(nudgeRef);
            thread = new Thread(this::run,
                    dispatchConcurrency == 1 ? "seg-proc-" + group : "seg-proc-" + group + "-s" + slot);
            thread.setDaemon(true);
            thread.start();
        }

        /** The owned segments this slot is responsible for. */
        private Set<Integer> mySegments() {
            Set<Integer> mine = new HashSet<>();
            for (Integer seg : ownedSegments) {
                if (Math.floorMod(seg, dispatchConcurrency) == slot) mine.add(seg);
            }
            return mine;
        }

        /** Block until the worker is between batches — used as the release/quiesce barrier. */
        void barrier() {
            synchronized (gate) {
                // Intentionally empty: acquiring the gate proves no batch is in flight.
            }
        }

        /** Rewind the owned segments' checkpoints + cursors for replay, under the gate. */
        void reset(Set<Integer> segs, long checkpoint) {
            synchronized (gate) {
                for (int seg : segs) {
                    if (Math.floorMod(seg, dispatchConcurrency) != slot) continue;
                    checkpointStore.reset(group, seg, seg, checkpoint);
                    cursor.remove(seg);
                }
            }
        }

        void run() {
            try {
                while (running.get()) {
                    boolean didWork;
                    try {
                        // Alive-heartbeat (throttled): if this stops printing while segments are
                        // still owned, the worker is blocked in the loop body (read/dispatch/save),
                        // not merely idle. If it keeps printing with a stuck cursor while newer
                        // events exist, the tail read is not returning them.
                        long nowMs = System.currentTimeMillis();
                        if (nowMs - lastHeartbeatMs >= 2_000L) {
                            lastHeartbeatMs = nowMs;
                            LOGGER.trace("pump alive group={} slot={} owned={} cursors={}",
                                    group, slot, mySegments(), cursor);
                        }
                        didWork = pollOnce() > 0;
                    } catch (Exception e) {
                        // A transient store/checkpoint failure must NOT kill the worker:
                        // the cluster keeps these segments parked-and-owned, so a dead
                        // worker would strand every later event for this group. Handler
                        // errors are swallowed into the DLQ inside dispatchProjection, so
                        // this only catches pump-loop failures. Back off and retry from the
                        // last durable checkpoint.
                        LOGGER.warn("projection worker group={} slot={} transient error, retrying: {}",
                                group, slot, e.getMessage());
                        Sleeper.sleep(ERROR_BACKOFF_MS);
                        continue;
                    }
                    if (!didWork) {
                        // Park until a local append nudges us or the backstop expires
                        // (cross-node events arrive only via the timed poll). A nudge
                        // landing between the empty poll above and this acquire left a
                        // permit, so we re-poll immediately — no lost-wakeup window.
                        try {
                            wakeup.tryAcquire(BACKSTOP_MS, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        wakeup.drainPermits();
                    }
                }
            } finally {
                PumpNudger.unregister(nudgeRef);
            }
        }

        /**
         * One wide read over the owned segments plus dispatch, under the gate.
         * Returns the number of events dispatched (0 ⇒ the caller may park).
         * The durable checkpoint advances once per touched segment at batch end —
         * still under the gate, so {@link #barrier()} keeps observing
         * "dispatched ⇒ checkpointed". A crash/handoff mid-batch re-processes at
         * most this batch on the new owner (at-least-once, as documented).
         */
        private int pollOnce() {
            int dispatched = 0;
            synchronized (gate) {
                Set<Integer> mine = mySegments();
                // Drop cursors for segments we no longer own (released/handoff).
                cursor.keySet().retainAll(mine);
                if (mine.isEmpty()) {
                    return 0;
                }
                Map<Integer, Long> readMap = new HashMap<>();
                for (int seg : mine) {
                    readMap.put(seg, cursor.computeIfAbsent(seg,
                            s -> checkpointStore.load(group, s, s)));
                }
                long tailStart = System.nanoTime();
                List<InternalMessage> batch = eventStore.loadSegmentsTail(readMap, batchLimit);
                Observability.get().onSegmentTailRead(group, batch.size(), System.nanoTime() - tailStart);
                if (LOGGER.isDebugEnabled() && !batch.isEmpty()) {
                    LOGGER.trace("pump group={} slot={} owned={} cursors={} read {} event(s)",
                            group, slot, mine, readMap, batch.size());
                }
                // Highest segment_seq dispatched per segment in this batch — saved once
                // at batch end instead of one UPSERT per event. The save runs in a
                // finally so a mid-batch dispatch failure still checkpoints the events
                // that DID dispatch (the in-memory cursor has already moved past them).
                Map<Integer, Long> maxDispatched = new HashMap<>();
                try {
                    for (InternalMessage m : batch) {
                        if (!running.get()) break;
                        UUID agg = m.getContext().getAggregateId();
                        if (agg == null) continue;
                        int seg = SegmentCalculator.calculateSegment(agg);
                        // Dispatch-time ownership gate: skip events for segments
                        // no longer owned (or not this slot's). In-flight events
                        // already dispatched are unaffected.
                        if (!ownedSegments.contains(seg)
                                || Math.floorMod(seg, dispatchConcurrency) != slot) {
                            LOGGER.trace("pump group={} slot={} SKIP agg={} seg={} seq={} "
                                            + "(owned={}, slotMatch={})",
                                    group, slot, agg, seg, m.getSegmentSeq(),
                                    ownedSegments.contains(seg),
                                    Math.floorMod(seg, dispatchConcurrency) == slot);
                            continue;
                        }
                        LOGGER.trace("pump group={} slot={} DISPATCH agg={} seg={} seq={}",
                                group, slot, agg, seg, m.getSegmentSeq());
                        eventManager.dispatchProjection(group, seg, m);
                        cursor.put(seg, m.getSegmentSeq());
                        maxDispatched.merge(seg, m.getSegmentSeq(), Math::max);
                        dispatched++;
                    }
                } finally {
                    // Checkpoint-after-process, batched: a crash/handoff before this
                    // point re-processes the un-checkpointed slice of the batch on the
                    // new owner. The store's monotonic save (GREATEST) means an
                    // overlapping owner cannot regress the cursor.
                    for (Map.Entry<Integer, Long> e : maxDispatched.entrySet()) {
                        checkpointStore.save(group, e.getKey(), e.getKey(), e.getValue());
                        Observability.get().onCheckpointSaved(group, e.getKey());
                        LOGGER.trace("pump group={} slot={} CHECKPOINTED seg={} seq={}",
                                group, slot, e.getKey(), e.getValue());
                    }
                }
            }
            return dispatched;
        }
    }

    // ---- saga worker: still one thread per owned (group, segment) ----

    private final class SagaWorker {
        final String group;
        final int segment;
        final AtomicBoolean running = new AtomicBoolean(true);
        Thread thread;

        SagaWorker(String group, int segment) {
            this.group = group;
            this.segment = segment;
        }

        void startThread() {
            thread = new Thread(this::run, "seg-proc-" + group + "-" + segment + "-saga");
            thread.setDaemon(true);
            thread.start();
        }

        void run() {
            try {
                new SagaSegmentWorker(eventManager, eventStore, checkpointStore, group, segment, segments,
                        batchLimit, running).run();
            } catch (Throwable t) {
                LOGGER.error("saga worker group={} seg={} crashed", group, segment, t);
            }
        }
    }
}
