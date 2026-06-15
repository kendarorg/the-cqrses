package org.kendar.cqrses.observability;

/**
 * Static holder for the process-wide {@link ObservabilityInterface}.
 *
 * <p>Mirrors the {@code EventApplyer} / {@code SagaManager} static-accessor
 * pattern already used on the dispatch path: a single {@code volatile} read with
 * no {@code GlobalRegistry} map lookup, so per-message instrumentation stays
 * effectively free. The default is {@link NullObservability}; {@code kf-spring}
 * installs its {@code MicrometerTimers} via {@link #set} during bootstrap and
 * clears it (back to no-op) on shutdown.
 *
 * <p>Call sites read {@code Observability.get().onX(...)}. {@code set(null)}
 * restores the no-op so {@link #get()} never returns {@code null}.
 */
public final class Observability {

    private static volatile ObservabilityInterface instance = new NullObservability();

    private Observability() {
    }

    public static ObservabilityInterface get() {
        return instance;
    }

    public static void set(ObservabilityInterface observability) {
        instance = (observability == null) ? new NullObservability() : observability;
    }
}
