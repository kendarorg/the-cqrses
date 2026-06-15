package org.kendar.cqrses.observability;

/**
 * One timed stage inside a sampled command trace. {@code detail} is a free
 * stage-specific scalar (events replayed, envelopes published, retry attempt,
 * ...) — {@code 0} when the stage has none.
 */
public record PerfStage(String stage, long nanos, long detail) {
}
