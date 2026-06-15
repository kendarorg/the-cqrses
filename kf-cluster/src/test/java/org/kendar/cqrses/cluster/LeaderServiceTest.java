package org.kendar.cqrses.cluster;

import org.junit.jupiter.api.Test;
import org.kendar.cqrses.db.Db;
import org.kendar.cqrses.db.DefaultDb;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeaderServiceTest extends ClusterTestBase {

    /** A {@link LeaderLock} stub: always held, epoch monotonic (mirrors the real DB lock per tick). */
    private static final class TestLock implements LeaderLock {
        boolean held = true;
        boolean increment = true;
        long epoch = 0;

        @Override
        public long acquire() {
            if (!held) return -1;
            return increment ? ++epoch : epoch;
        }

        @Override
        public boolean isHeld() {
            return held;
        }

        @Override
        public void release() {
            held = false;
        }
    }

    /** Leader with the HTTP probe/poke seams stubbed: responsiveness keyed by liveness port. */
    private static final class TestLeader extends LeaderService {
        final Set<Integer> responsivePorts = new HashSet<>();
        final List<Integer> pokedPorts = new ArrayList<>();

        TestLeader(Db db, ClusterClock clock, LeaderLock lock, int itemCount) {
            super(db, "LEADER", clock, lock, itemCount);
        }

        @Override
        protected boolean probeAlive(String host, int port) {
            return responsivePorts.contains(port);
        }

        @Override
        protected void poke(String host, int port) {
            pokedPorts.add(port);
        }
    }

    private int countOwnedBy(String node) {
        return db.queryForObject(
                "SELECT COUNT(*) FROM cluster_assignments WHERE owner_node = ?", Long.class, node).intValue();
    }

    @Test
    void coldStartBalancesAcrossLiveNodes() {
        putNode("A", "h", 1, clock.now());
        putNode("B", "h", 2, clock.now());
        putNode("C", "h", 3, clock.now());
        TestLeader leader = new TestLeader(db, clock, new TestLock(), 9);

        leader.leaderTick();

        assertEquals(3, countOwnedBy("A"));
        assertEquals(3, countOwnedBy("B"));
        assertEquals(3, countOwnedBy("C"));
        // Brand-new assignments poke every affected node.
        assertFalse(leader.pokedPorts.isEmpty());
    }

    @Test
    void healthRowIsStamped() {
        putNode("A", "h", 1, clock.now());
        TestLeader leader = new TestLeader(db, clock, new TestLock(), 2);
        leader.leaderTick();

        Long lastTick = db.queryForObject(
                "SELECT last_tick FROM cluster_leader_health WHERE id = 1", Long.class);
        assertEquals(clock.now(), lastTick);
    }

    @Test
    void staleLeaderWriteRejectedByEpochFence() {
        // A row already written by a real leader at epoch 5.
        putNode("A", "h", 1, clock.now());
        db.insertInto("cluster_assignments")
                .set("item_id", 0).set("owner_node", "STALE").set("epoch", 5L).execute();
        db.insertInto("cluster_assignments")
                .set("item_id", 1).set("owner_node", "STALE").set("epoch", 5L).execute();

        // A stale leader stuck at epoch 2 tries to take ownership.
        TestLock stale = new TestLock();
        stale.increment = false;
        stale.epoch = 2;
        TestLeader leader = new TestLeader(db, clock, stale, 2);

        leader.leaderTick();

        // Both rows keep their higher-epoch ownership; the lower-epoch write was dropped.
        assertEquals("STALE", ownerOf(0));
        assertEquals("STALE", ownerOf(1));
        assertEquals(5L, epochOf(0));
    }

    @Test
    void flappingNodeWaitsAFullStabilizationWindow() {
        // Cold start with A,B only — C is absent so it is not part of the cold assignment.
        putNode("A", "h", 1, clock.now());
        putNode("B", "h", 2, clock.now());
        TestLeader leader = new TestLeader(db, clock, new TestLock(), 6);
        leader.leaderTick();
        assertEquals(0, countOwnedBy("C"));

        // C appears; a sub-window tick must NOT assign to it.
        clock.advance(1000);
        refresh("A", 1, "B", 2, "C", 3);
        leader.leaderTick();
        assertEquals(0, countOwnedBy("C"), "C must not be assigned within the stabilization window");

        // Membership {A,B,C} now stable for a full window → C is assigned.
        clock.advance(ClusterConfig.MEMBERSHIP_STABILIZE + 1);
        refresh("A", 1, "B", 2, "C", 3);
        leader.leaderTick();
        assertTrue(countOwnedBy("C") > 0, "C should be assigned once membership is stable");
    }

    @Test
    void continuousInstabilityForcesRebalanceOverContinuouslyPresentNodes() {
        putNode("A", "h", 1, clock.now());
        putNode("B", "h", 2, clock.now());
        TestLeader leader = new TestLeader(db, clock, new TestLock(), 6);
        leader.leaderTick(); // cold start over {A,B}

        // Flap C in and out every tick so membership never stabilises. We DELETE C's row when it
        // is "out" because the staleness window (9s) is wider than a tick (5s), so merely not
        // refreshing would still leave C live for a tick.
        boolean cIn = true;
        for (int i = 0; i < 10; i++) {
            clock.advance(ClusterConfig.LEADER_TICK);
            refresh("A", 1, "B", 2);
            if (cIn) {
                refresh("C", 3);
            } else {
                db.update("DELETE FROM cluster_nodes WHERE node_id = ?", "C");
            }
            cIn = !cIn;
            leader.leaderTick();
        }

        // A flapping C is never granted partitions; A and B (continuously present) own everything.
        assertEquals(0, countOwnedBy("C"));
        assertEquals(6, countOwnedBy("A") + countOwnedBy("B"));
    }

    @Test
    void staleButResponsiveNodeIsSpared() {
        putNode("A", "h", 1, clock.now());
        putNode("B", "h", 2, clock.now());
        TestLeader leader = new TestLeader(db, clock, new TestLock(), 4);
        leader.responsivePorts.add(2); // B answers /alive
        leader.leaderTick();
        int bShare = countOwnedBy("B");
        assertTrue(bShare > 0);

        // B stops heartbeating but keeps answering /alive across several ticks.
        for (int i = 0; i < 3; i++) {
            clock.advance(ClusterConfig.LEADER_TICK);
            refresh("A", 1); // only A heartbeats
            leader.leaderTick();
        }
        assertEquals(bShare, countOwnedBy("B"), "an alive-but-DB-blind node must keep its partitions");
    }

    @Test
    void unresponsiveNodeIsReassignedAfterConfirmationAndStabilization() {
        putNode("A", "h", 1, clock.now());
        putNode("B", "h", 2, clock.now());
        TestLeader leader = new TestLeader(db, clock, new TestLock(), 4);
        // B is NOT in responsivePorts → probe fails.
        leader.leaderTick();
        assertTrue(countOwnedBy("B") > 0);

        // Tick 1: B stale, first failed probe (fails=1 < threshold) → spared, still owns its share.
        clock.advance(ClusterConfig.STALENESS_WINDOW + 1);
        refresh("A", 1);
        leader.leaderTick();
        assertTrue(countOwnedBy("B") > 0, "one failed probe is not yet confirmation of death");

        // Tick 2: second failed probe (fails=2) confirms death → membership drops to {A}, which
        // itself resets the stability gate, so reassignment does not happen on this tick yet.
        clock.advance(ClusterConfig.LEADER_TICK);
        refresh("A", 1);
        leader.leaderTick();

        // Tick 3: {A} has now been the stable membership for a full window → B's items move to A.
        clock.advance(ClusterConfig.MEMBERSHIP_STABILIZE + 1);
        refresh("A", 1);
        leader.leaderTick();

        assertEquals(0, countOwnedBy("B"), "a confirmed-dead node's partitions are reassigned");
        assertEquals(4, countOwnedBy("A"));
    }

    @Test
    void dbFailureMakesTickANoOpWithoutThrowing() {
        // A Db whose connection() always throws — every statement fails.
        Db failing = new DefaultDb(brokenDataSource());
        TestLeader leader = new TestLeader(failing, clock, new TestLock(), 4);
        assertDoesNotThrow(leader::leaderTick);

        // Nothing was written (assert against the real, healthy db: still no assignments).
        assertNull(ownerOf(0));
    }

    private DataSource brokenDataSource() {
        return new DataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                throw new SQLException("db down");
            }

            @Override
            public Connection getConnection(String u, String p) throws SQLException {
                throw new SQLException("db down");
            }

            @Override
            public java.io.PrintWriter getLogWriter() {
                return null;
            }

            @Override
            public void setLogWriter(java.io.PrintWriter out) {
            }

            @Override
            public void setLoginTimeout(int seconds) {
            }

            @Override
            public int getLoginTimeout() {
                return 0;
            }

            @Override
            public java.util.logging.Logger getParentLogger() {
                return java.util.logging.Logger.getGlobal();
            }

            @Override
            public <T> T unwrap(Class<T> iface) {
                return null;
            }

            @Override
            public boolean isWrapperFor(Class<?> iface) {
                return false;
            }
        };
    }

    /** Refresh the given (nodeId, port) pairs to heartbeat = now; others are left to go stale. */
    private void refresh(Object... nodeIdPortPairs) {
        for (int i = 0; i < nodeIdPortPairs.length; i += 2) {
            putNode((String) nodeIdPortPairs[i], "h", (Integer) nodeIdPortPairs[i + 1], clock.now());
        }
    }
}
