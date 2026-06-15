package org.kendar.cqrses.cluster.spi;

/**
 * kf-core SPI the cluster adapter drives. A cluster node that owns segment
 * {@code i} (== partition {@code i} == {@code itemId}) owns segment {@code i} of
 * <i>every</i> event-side processing group. The implementation
 * ({@code SegmentProcessor}) is the source of event-side dispatch for owned
 * segments — it polls the event store tail and feeds the (thread-less, pull-mode)
 * lanes.
 * <p>
 * The dependency direction is preserved: kf-cluster → kf-core. kf-core never
 * imports kf-cluster; {@code onDrained} is how kf-core signals "I have truly
 * stopped" without knowing about {@code ClusterNode.release}.
 */
public interface SegmentOwnership {

    /**
     * Begin processing {@code segment} — start the per-{@code (group, segment)}
     * worker threads for every event-side group and <b>block</b> until the segment
     * is released. The cluster pump thread parks here for the lifetime of
     * ownership (matching {@code ItemProcessor.process}, which must not return
     * under normal operation).
     */
    void claimSegment(int segment);

    /**
     * Stop processing {@code segment}: signal the workers to stop, drain the
     * in-flight dispatch + last checkpoint, join the threads, then run
     * {@code onDrained}. Returns promptly (the drain runs off the caller's thread)
     * so the cluster can keep the lease alive until {@code onDrained} fires.
     */
    void releaseSegment(int segment, Runnable onDrained);
}
