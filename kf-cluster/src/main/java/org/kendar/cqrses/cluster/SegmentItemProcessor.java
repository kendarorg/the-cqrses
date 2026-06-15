package org.kendar.cqrses.cluster;

import org.kendar.cqrses.cluster.spi.SegmentOwnership;

/**
 * The thin kf-cluster ↔ kf-core adapter: maps the cluster's partition lifecycle
 * onto the kf-core {@link SegmentOwnership} pull engine ({@code SegmentProcessor}).
 * Because {@code itemId == segment}, owning partition {@code i} means owning
 * segment {@code i} of every event-side processing group.
 * <p>
 * Dependency direction stays kf-cluster → kf-core: this class imports the kf-core
 * SPI and its own {@code ClusterNode}; kf-core never imports kf-cluster. The
 * drained-callback wiring ({@code node.release(seg)} only after kf-core confirms
 * the drain) is the cooperative {@code stopProcess → release} lifecycle in
 * {@code docs/tricks.md} §40.
 */
public final class SegmentItemProcessor implements ItemProcessor {

    private final SegmentOwnership ownership;
    private final ClusterNode node;

    public SegmentItemProcessor(SegmentOwnership ownership, ClusterNode node) {
        this.ownership = ownership;
        this.node = node;
    }

    /**
     * Claim segment {@code itemId} and <b>park</b> until released —
     * {@link SegmentOwnership#claimSegment} blocks the cluster pump thread for the
     * lifetime of ownership, satisfying the {@code ItemProcessor.process} contract
     * (must not return under normal operation).
     */
    @Override
    public void process(int itemId) {
        ownership.claimSegment(itemId);
    }

    /**
     * Wind segment {@code itemId} down. Returns promptly; the engine drains off the
     * caller's thread and only then runs {@code node.release(itemId)} to clear the
     * lease, so the gaining node cannot start a second pump until this one has
     * truly stopped.
     */
    @Override
    public void stopProcess(int itemId) {
        ownership.releaseSegment(itemId, () -> node.release(itemId));
    }
}
