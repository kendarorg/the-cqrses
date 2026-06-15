package org.kendar.cqrses.observability;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded in-memory {@link TraceSink}: a drop-oldest ring of the most recent
 * {@code capacity} traces. Deliberately the <b>only</b> persistence for perf
 * traces — writing them to the framework database would perturb the very
 * measurements being taken. A harness (the cluster IT) harvests the buffer
 * over HTTP via {@link #snapshot()} and reports {@link #dropped()} so silent
 * truncation stays visible.
 *
 * <p>{@code accept} never blocks and never throws: on overflow it evicts the
 * oldest trace and counts the eviction.
 */
public class InMemoryTraceSink implements TraceSink {

    private final ArrayBlockingQueue<PerfTrace> buffer;
    private final AtomicLong dropped = new AtomicLong();

    public InMemoryTraceSink(int capacity) {
        this.buffer = new ArrayBlockingQueue<>(Math.max(1, capacity));
    }

    @Override
    public void accept(PerfTrace trace) {
        if (trace == null) {
            return;
        }
        while (!buffer.offer(trace)) {
            if (buffer.poll() != null) {
                dropped.incrementAndGet();
            }
        }
    }

    /** Immutable copy of the buffered traces, oldest first. */
    public List<PerfTrace> snapshot() {
        return List.copyOf(new ArrayList<>(buffer));
    }

    /** Traces evicted by overflow since construction (or the last {@link #clear()}). */
    public long dropped() {
        return dropped.get();
    }

    public void clear() {
        buffer.clear();
        dropped.set(0);
    }
}
