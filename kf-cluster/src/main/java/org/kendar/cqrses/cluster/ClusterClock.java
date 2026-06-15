package org.kendar.cqrses.cluster;

/**
 * Injectable wall-clock time source, in epoch milliseconds. The default delegates
 * to {@link System#currentTimeMillis()}; tests pass a controllable clock so they can
 * advance time without sleeping. Every kf-cluster service threads its time through
 * this seam.
 * <p>
 * Wall-clock comparison across nodes assumes loosely-synced clocks (NTP) — a
 * documented assumption. Margins are deliberately generous (see {@link ClusterConfig})
 * and the {@code /alive} probe backstops a skew-induced false death.
 */
@FunctionalInterface
public interface ClusterClock {

    long now();

    ClusterClock SYSTEM = System::currentTimeMillis;
}
