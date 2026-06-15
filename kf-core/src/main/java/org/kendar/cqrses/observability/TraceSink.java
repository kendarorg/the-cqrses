package org.kendar.cqrses.observability;

/**
 * Consumer of completed {@link PerfTrace}s handed over by
 * {@link TraceRecorder#end(boolean)} on the command thread.
 *
 * <p>{@code accept} runs on the dispatch hot path and therefore MUST never
 * block and MUST never throw — drop the trace instead. The in-memory
 * {@link InMemoryTraceSink} is the reference implementation.
 */
public interface TraceSink {

    void accept(PerfTrace trace);

    default void close() {
    }
}
