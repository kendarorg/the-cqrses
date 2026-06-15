package org.kendar.cqrses.repositories;

import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.observability.Observability;
import org.kendar.cqrses.observability.TraceRecorder;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Per-segment group commit for the synchronous (own-connection) append path.
 * <p>
 * The store's append serialiser is the {@code segment_counter} row lock, held to
 * commit — so concurrent {@code sendSync} appends to one segment used to queue on
 * that DB lock and each pay a full commit/fsync of its own. Under flood the fsync
 * is the dominant cost (measured: commit p99 &gt; 300&nbsp;ms while handler time is
 * &lt; 1&nbsp;ms), and because the lock is held across it, a segment's throughput
 * collapses to {@code 1 / fsync}. This class folds the waiters into the holder's
 * transaction instead: requests for the same segment are queued, and whichever
 * caller thread holds the per-segment {@link ReentrantLock} drains the queue and
 * writes the whole batch in <b>one</b> transaction with <b>one</b> commit. The
 * fsync (and the row-lock hold) is amortised over the batch; batch size is
 * self-clocking — exactly the requests that arrived while the previous commit was
 * in flight, so an idle system degenerates to today's batch-of-1 with zero added
 * latency.
 *
 * <p><b>Reliability is unchanged.</b> A caller is released only after the commit
 * containing its rows returned (durable-before-ack); the batch transaction
 * acquires the same {@code segment_counter} row lock, so {@code segment_seq}
 * stays gap-free and assignment order == commit order across JVMs (the invariant
 * {@code JdbcEventStore#lockSegmentCounter} documents); per-request OCC failures
 * are isolated — see {@link BatchWriter}.
 *
 * <p>Used only when no transaction boundary is bound to the calling thread; the
 * boundary path ({@code JdbcProcessingGroup}) keeps its inline append because the
 * boundary owns the transaction.
 */
final class SegmentAppendCoalescer {

    /**
     * Hard cap on requests folded into one transaction, bounding transaction size
     * and the latency tail of the last request in a long queue.
     */
    private static final int MAX_BATCH = 64;

    /**
     * Writes one batch in a single transaction. The contract is <b>every request
     * is completed</b> ({@link Req#complete()} after its rows are durably
     * committed, or {@link Req#fail(Throwable)}) before returning; per-request
     * OCC conflicts must fail only the conflicting request. {@link #append} has a
     * backstop that fails any request left incomplete, so a buggy writer can
     * never strand a caller.
     */
    interface BatchWriter {
        void write(int segment, List<Req> batch);
    }

    /** One caller's append: the events of one aggregate, awaiting one commit. */
    static final class Req {
        private final UUID aggregateId;
        private final String group;
        private final List<InternalMessage> events;
        /**
         * Aggregate versions exactly as the caller sent them ({@code -1} = assign
         * next). Version assignment mutates the event contexts, so a request
         * re-executed after a batch rollback must first be restored to its
         * requested versions or a {@code -1} request would replay as an explicit
         * (now possibly stale) version and fail a spurious OCC check.
         */
        private final long[] requestedVersions;
        private Throwable error;
        private volatile boolean done;

        Req(UUID aggregateId, String group, List<InternalMessage> events) {
            this.aggregateId = aggregateId;
            this.group = group;
            this.events = events;
            this.requestedVersions = new long[events.size()];
            for (int i = 0; i < events.size(); i++) {
                requestedVersions[i] = events.get(i).getContext().getAggregateVersion();
            }
        }

        UUID aggregateId() {
            return aggregateId;
        }

        String group() {
            return group;
        }

        List<InternalMessage> events() {
            return events;
        }

        boolean isDone() {
            return done;
        }

        /** Marks success. No-op if the request already failed. */
        void complete() {
            done = true;
        }

        /** Marks failure. No-op if the request is already completed or failed. */
        void fail(Throwable t) {
            if (done) return;
            error = t;
            done = true;
        }

        void restoreRequestedVersions() {
            for (int i = 0; i < requestedVersions.length; i++) {
                events.get(i).getContext().setAggregateVersion(requestedVersions[i]);
            }
        }

        private void rethrow() {
            if (error == null) return;
            if (error instanceof RuntimeException re) throw re;
            if (error instanceof Error err) throw err;
            throw new IllegalStateException("append failed", error);
        }
    }

    private static final class Slot {
        // Fair, so a request enqueued early cannot be starved by barging arrivals
        // while the queue is hot; the per-acquisition cost is irrelevant next to
        // the millisecond-scale commit each critical section performs.
        final ReentrantLock lock = new ReentrantLock(true);
        final ConcurrentLinkedQueue<Req> queue = new ConcurrentLinkedQueue<>();
    }

    private final ConcurrentHashMap<Integer, Slot> slots = new ConcurrentHashMap<>();
    private final BatchWriter writer;

    SegmentAppendCoalescer(BatchWriter writer) {
        this.writer = writer;
    }

    /**
     * Appends {@code events} (one aggregate, already segment-routed) and blocks
     * until they are durably committed — by this thread or by another caller that
     * folded this request into its batch. Throws what the write failed with
     * (OCC / {@code DbException}), exactly like the inline append did.
     */
    void append(int segment, UUID aggregateId, String group, List<InternalMessage> events) {
        Slot slot = slots.computeIfAbsent(segment, s -> new Slot());
        Req req = new Req(aggregateId, group, events);
        slot.queue.add(req);
        Observability.get().onAppendInFlight(segment, +1);
        long waitStart = System.nanoTime();
        boolean led = false;
        try {
            slot.lock.lock();
            try {
                // Either a previous lock holder already wrote this request (done),
                // or it is still queued and this thread becomes the next batch
                // leader. A leader keeps draining until its own request is done —
                // with MAX_BATCH-capped batches the request may be a few batches in.
                while (!req.isDone()) {
                    List<Req> batch = drain(slot.queue);
                    if (batch.isEmpty()) {
                        // Unreachable — an undone request is still queued and this
                        // thread holds the lock. Defensive: never spin.
                        req.fail(new IllegalStateException(
                                "append request neither completed nor queued"));
                        break;
                    }
                    led = true;
                    try {
                        writer.write(segment, batch);
                    } finally {
                        // Backstop for a writer bug or an Error mid-batch: no
                        // caller may hang. fail() is a no-op on completed requests.
                        IllegalStateException incomplete = null;
                        for (Req r : batch) {
                            if (r.isDone()) continue;
                            if (incomplete == null) {
                                incomplete = new IllegalStateException(
                                        "append batch writer returned without completing the request");
                            }
                            r.fail(incomplete);
                        }
                    }
                }
            } finally {
                slot.lock.unlock();
            }
        } finally {
            Observability.get().onAppendInFlight(segment, -1);
            if (TraceRecorder.active()) {
                // Whole coalesced append as seen by this caller: for a leader it
                // brackets its own batch write, for a waiter it is the time spent
                // riding someone else's commit. detail: 1 = led, 0 = waited.
                TraceRecorder.stage("append.wait", System.nanoTime() - waitStart, led ? 1 : 0);
            }
        }
        req.rethrow();
    }

    private static List<Req> drain(ConcurrentLinkedQueue<Req> queue) {
        List<Req> out = new ArrayList<>();
        Req r;
        while (out.size() < MAX_BATCH && (r = queue.poll()) != null) {
            out.add(r);
        }
        return out;
    }
}
