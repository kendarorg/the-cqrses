package org.kendar.cqrses.cluster.spi;

/**
 * Static holder for the process-wide {@link CommandForwarder}.
 *
 * <p>Mirrors the {@link org.kendar.cqrses.observability.Observability} holder: a
 * single {@code volatile} read on the send hot path, no {@code GlobalRegistry}
 * map lookup. Unlike {@code Observability} there is no no-op default —
 * {@link #current()} returns {@code null} when nothing is installed and the bus
 * skips the hook entirely, so the single-node cost is one null check.
 *
 * <p>Install happens after {@code GlobalRegistry.start()} (the cluster node, and
 * with it the forwarder, starts after the buses) but must complete before the
 * first application {@code send}/{@code sendSync} — {@code KfBootstrap}'s
 * lifecycle ordering guarantees that. {@link #reset()} must run before the buses
 * stop so a late local send degrades to local dispatch instead of racing a
 * closing transport.
 */
public final class CommandForwarding {

    private static volatile CommandForwarder instance;

    private CommandForwarding() {
    }

    /** Nullable — null means "no forwarder installed, dispatch locally". */
    public static CommandForwarder current() {
        return instance;
    }

    public static void install(CommandForwarder forwarder) {
        instance = forwarder;
    }

    public static void reset() {
        instance = null;
    }
}
