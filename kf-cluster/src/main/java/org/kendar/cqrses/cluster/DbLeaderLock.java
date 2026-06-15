package org.kendar.cqrses.cluster;

import org.kendar.cqrses.db.Db;
import org.kendar.cqrses.db.DbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default DB-backed {@link LeaderLock}: a single atomic compare-and-swap on the singleton
 * {@code cluster_leader_lock} row. The epoch increment is part of the same write that claims the
 * row, so the returned epoch is a strictly-monotonic fencing token — a revived old leader always
 * carries a lower token than the current one.
 * <p>
 * <b>Fail-safe:</b> any DB error makes the node behave as <i>not</i> holding the lock
 * ({@code acquire()} → {@code -1}, {@code isHeld()} → {@code false}); the lock-lease then simply
 * expires and the cluster is briefly leaderless (a stall, not a split brain).
 */
public class DbLeaderLock implements LeaderLock {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbLeaderLock.class.getName());

    private final Db db;
    private final String nodeId;
    private final ClusterClock clock;

    public DbLeaderLock(Db db, String nodeId, ClusterClock clock) {
        this.db = db;
        this.nodeId = nodeId;
        this.clock = clock;
    }

    @Override
    public long acquire() {
        long now = clock.now();
        try {
            // Only an unclaimed / expired / own row can be taken; epoch increments atomically.
            int won = db.update("""
                    UPDATE cluster_leader_lock
                       SET owner_node = ?, epoch = epoch + 1, lease_until = ?
                     WHERE id = 1 AND (owner_node IS NULL OR owner_node = ? OR lease_until < ?)
                    """, nodeId, now + ClusterConfig.LEADER_LOCK_LEASE, nodeId, now);
            if (won != 1) {
                return -1;
            }
            Long epoch = db.queryForObject(
                    "SELECT epoch FROM cluster_leader_lock WHERE id = 1", Long.class);
            return epoch == null ? -1 : epoch;
        } catch (DbException e) {
            LOGGER.warn("leader-lock acquire failed, treating as not-leader: {}", e.getMessage());
            return -1;
        }
    }

    @Override
    public boolean isHeld() {
        long now = clock.now();
        try {
            Long epoch = db.queryForObject(
                    "SELECT epoch FROM cluster_leader_lock WHERE id = 1 AND owner_node = ? AND lease_until > ?",
                    Long.class, nodeId, now);
            return epoch != null;
        } catch (DbException e) {
            return false;
        }
    }

    @Override
    public void release() {
        try {
            db.update(
                    "UPDATE cluster_leader_lock SET owner_node = NULL, lease_until = 0 WHERE id = 1 AND owner_node = ?",
                    nodeId);
        } catch (DbException e) {
            LOGGER.debug("leader-lock release swallowed: {}", e.getMessage());
        }
    }
}
