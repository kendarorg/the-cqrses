package org.kendar.cqrses.observability;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sampled per-command trace collector. {@link #begin} takes the 1-in-N sampling
 * decision once per command at the {@code sendSync} entry point; when sampled,
 * the whole synchronous pipeline (rehydrate → handler → append phases →
 * publish) runs on the caller's thread, so a plain {@link ThreadLocal} carries
 * the active trace with no propagation.
 *
 * <p>Cost when disabled: {@code begin} is one volatile read; {@code stage} is
 * one ThreadLocal read (the ThreadLocal is only ever set for sampled commands).
 * Allocation happens exclusively on the sampled path.
 *
 * <p>{@link #install} follows the frozen-topology lifecycle rule: setup phase
 * only, before any {@code send}/{@code publish} is in flight; {@link #reset}
 * on shutdown.
 */
public final class TraceRecorder {

    private record Config(TraceSink sink, int sampleEvery) {
    }

    private static final class ActiveTrace {
        final UUID traceId;
        final String commandType;
        final UUID aggregateId;
        final long startedAtMillis;
        final long startNanos;
        final List<PerfStage> stages = new ArrayList<>(12);

        ActiveTrace(UUID traceId, String commandType, UUID aggregateId) {
            this.traceId = traceId;
            this.commandType = commandType;
            this.aggregateId = aggregateId;
            this.startedAtMillis = System.currentTimeMillis();
            this.startNanos = System.nanoTime();
        }
    }

    private static volatile Config config;
    private static final AtomicLong COUNTER = new AtomicLong();
    private static final ThreadLocal<ActiveTrace> CURRENT = new ThreadLocal<>();

    private TraceRecorder() {
    }

    /**
     * Install the sink and the 1-in-N sampling rate. Setup phase only.
     * {@code sampleEvery <= 0} is normalised to 1 (trace everything).
     */
    public static void install(TraceSink sink, int sampleEvery) {
        config = (sink == null) ? null : new Config(sink, Math.max(1, sampleEvery));
    }

    /** Back to disabled; pending thread-local state is abandoned harmlessly. */
    public static void reset() {
        config = null;
    }

    /**
     * Sampling decision for one command. Returns {@code true} iff this command
     * is traced — the caller must then invoke {@link #end(boolean)} in a
     * {@code finally}. Defensively clears any stale trace a previous command
     * leaked by dying between begin and end.
     */
    public static boolean begin(UUID traceId, String commandType, UUID aggregateId) {
        var cfg = config;
        if (cfg == null) {
            return false;
        }
        if (CURRENT.get() != null) {
            CURRENT.remove();
        }
        if (COUNTER.getAndIncrement() % cfg.sampleEvery != 0) {
            return false;
        }
        CURRENT.set(new ActiveTrace(traceId, commandType, aggregateId));
        return true;
    }

    /**
     * Whether the current thread's command is being traced. Lets call sites skip
     * building stage labels (string concat) on the not-sampled path.
     */
    public static boolean active() {
        return CURRENT.get() != null;
    }

    public static void stage(String stage, long nanos) {
        stage(stage, nanos, 0L);
    }

    public static void stage(String stage, long nanos, long detail) {
        var active = CURRENT.get();
        if (active == null) {
            return;
        }
        active.stages.add(new PerfStage(stage, nanos, detail));
    }

    /**
     * Append the synthetic {@code total} stage, hand the trace to the sink and
     * clear the thread-local. The sink contract (never block / never throw) is
     * defended with a catch-all so a misbehaving sink cannot fail the command.
     */
    public static void end(boolean ok) {
        var active = CURRENT.get();
        if (active == null) {
            return;
        }
        CURRENT.remove();
        var cfg = config;
        if (cfg == null) {
            return;
        }
        active.stages.add(new PerfStage("total", System.nanoTime() - active.startNanos, active.stages.size()));
        try {
            cfg.sink.accept(new PerfTrace(active.traceId, active.commandType, active.aggregateId,
                    active.startedAtMillis, ok, List.copyOf(active.stages)));
        } catch (Exception ignored) {
            // a trace must never fail the command it observed
        }
    }
}
