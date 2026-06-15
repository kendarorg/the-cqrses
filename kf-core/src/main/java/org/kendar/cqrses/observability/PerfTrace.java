package org.kendar.cqrses.observability;

import java.util.List;
import java.util.UUID;

/**
 * A sampled end-to-end command trace: the ordered stages one {@code sendSync}
 * walked through (rehydrate, handler, append phases, publish, ...) plus the
 * synthetic terminal {@code total} stage added by {@link TraceRecorder#end}.
 * Collected by a {@link TraceSink}; deliberately never persisted to the
 * framework database — perf data writing to the measured store would perturb
 * the measurement.
 */
public record PerfTrace(UUID traceId, String commandType, UUID aggregateId,
                        long startedAtMillis, boolean ok, List<PerfStage> stages) {
}
