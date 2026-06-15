package org.kendar.cqrses.cluster;

/**
 * Leader-election fencing lock SPI. The default {@link DbLeaderLock} is DB-backed (a CAS on a
 * single leader row, with a monotonic epoch as the fencing token) so kf-cluster works out of the
 * box and is fully testable. Anyone wanting Ratis / ZooKeeper / etcd plugs in their own
 * implementation without touching kf-cluster internals.
 */
public interface LeaderLock {

    /**
     * Attempt to acquire or renew leadership.
     *
     * @return the new monotonic fencing epoch on success, or {@code -1} on failure (someone else
     * holds an unexpired lock, or the backing store is unreachable).
     */
    long acquire();

    /**
     * @return whether this node currently holds an unexpired lock.
     */
    boolean isHeld();

    /**
     * Relinquish leadership if held. Idempotent; best-effort.
     */
    void release();
}
