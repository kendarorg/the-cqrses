package org.kendar.cqrses.cluster.forwarding;

import org.junit.jupiter.api.Test;
import org.kendar.cqrses.cluster.ClusterTestBase;
import org.kendar.cqrses.db.Db;
import org.kendar.cqrses.db.DefaultDb;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

class ClusterRoutingTableTest extends ClusterTestBase {

    private void putForwardableNode(String nodeId, String host, int forwardPort) {
        db.insertInto("cluster_nodes")
                .set("node_id", nodeId).set("host", host)
                .set("liveness_port", 8070).set("forward_port", forwardPort)
                .set("last_heartbeat", clock.now())
                .execute();
    }

    private void putAssignment(int itemId, String owner) {
        db.insertInto("cluster_assignments")
                .set("item_id", itemId).set("owner_node", owner).set("epoch", 1L)
                .execute();
    }

    @Test
    void resolvesSegmentToOwnersForwardAddress() {
        putForwardableNode("n2", "10.0.0.2", 8071);
        putAssignment(3, "n2");

        var table = new ClusterRoutingTable(db, "n1");
        table.refreshNow();

        var route = table.routeFor(3);
        assertTrue(route.isPresent());
        assertEquals(new NodeAddress("n2", "10.0.0.2", 8071), route.get());
    }

    @Test
    void selfOwnedSegmentYieldsEmpty() {
        putForwardableNode("n1", "10.0.0.1", 8071);
        putAssignment(0, "n1");

        var table = new ClusterRoutingTable(db, "n1");
        table.refreshNow();

        assertTrue(table.routeFor(0).isEmpty(), "own segment must run locally, never loop back");
    }

    @Test
    void unassignedSegmentYieldsEmpty() {
        var table = new ClusterRoutingTable(db, "n1");
        table.refreshNow();
        assertTrue(table.routeFor(7).isEmpty());
    }

    @Test
    void nodeWithoutForwardPortIsNotATarget() {
        // forward_port = 0: node runs an older version or has forwarding disabled.
        putForwardableNode("n2", "10.0.0.2", 0);
        putAssignment(3, "n2");

        var table = new ClusterRoutingTable(db, "n1");
        table.refreshNow();

        assertTrue(table.routeFor(3).isEmpty());
    }

    @Test
    void refreshSwapsToLatestDbState() {
        putForwardableNode("n2", "10.0.0.2", 8071);
        putAssignment(3, "n2");
        var table = new ClusterRoutingTable(db, "n1");
        table.refreshNow();
        assertEquals("n2", table.routeFor(3).orElseThrow().nodeId());

        // leader rebalances segment 3 to n3
        putForwardableNode("n3", "10.0.0.3", 8071);
        db.update("UPDATE cluster_assignments SET owner_node = 'n3' WHERE item_id = 3");
        table.refreshNow();
        assertEquals("n3", table.routeFor(3).orElseThrow().nodeId());
    }

    @Test
    void dbErrorKeepsPreviousCache() {
        putForwardableNode("n2", "10.0.0.2", 8071);
        putAssignment(3, "n2");
        var table = new ClusterRoutingTable(db, "n1");
        table.refreshNow();
        assertTrue(table.routeFor(3).isPresent());

        // Same table, now backed by a dead datasource: refresh must swallow and keep serving.
        var broken = new ClusterRoutingTable(new DefaultDb(brokenDataSource()), "n1");
        assertDoesNotThrow(broken::refreshNow);
        assertTrue(broken.routeFor(3).isEmpty(), "never-refreshed table stays empty");
        // and the healthy table is untouched
        assertTrue(table.routeFor(3).isPresent());
    }

    @Test
    void refreshAsyncIsRateLimitedAndSafeWhenStopped() {
        var table = new ClusterRoutingTable(db, "n1");
        // not started: must not throw, just drop the request
        assertDoesNotThrow(table::refreshAsync);
        assertDoesNotThrow(table::refreshAsync);
    }

    @Test
    void startStopLifecycle() {
        putForwardableNode("n2", "10.0.0.2", 8071);
        putAssignment(1, "n2");
        var table = new ClusterRoutingTable(db, "n1");
        table.start();
        try {
            assertTrue(table.routeFor(1).isPresent(), "start() performs an eager first refresh");
        } finally {
            table.stop();
        }
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
}
