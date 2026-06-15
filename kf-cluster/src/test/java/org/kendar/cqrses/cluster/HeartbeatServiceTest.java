package org.kendar.cqrses.cluster;

import org.junit.jupiter.api.Test;
import org.kendar.cqrses.db.Db;
import org.kendar.cqrses.db.DefaultDb;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class HeartbeatServiceTest extends ClusterTestBase {

    private Long heartbeatOf(String nodeId) {
        return db.queryForObject(
                "SELECT last_heartbeat FROM cluster_nodes WHERE node_id = ?", Long.class, nodeId);
    }

    @Test
    void heartbeatOnceUpsertsAndRefreshes() {
        HeartbeatService hb = new HeartbeatService(db, "A", "host", 5000, clock);

        hb.heartbeatOnce();
        assertEquals(clock.now(), heartbeatOf("A"));

        clock.advance(3000);
        hb.heartbeatOnce();
        assertEquals(clock.now(), heartbeatOf("A"));

        assertEquals(1, (long) db.queryForObject(
                "SELECT COUNT(*) FROM cluster_nodes WHERE node_id = 'A'", Long.class));
    }

    @Test
    void heartbeatCarriesForwardPortOnInsertAndUpdate() {
        HeartbeatService hb = new HeartbeatService(db, "A", "host", 5000, 6000, clock);

        hb.heartbeatOnce();
        assertEquals(6000, (int) db.queryForObject(
                "SELECT forward_port FROM cluster_nodes WHERE node_id = 'A'", Integer.class));

        // The update path must keep re-asserting the port (a restart with a new
        // port advertises it on the first refresh of the existing row).
        HeartbeatService hb2 = new HeartbeatService(db, "A", "host", 5000, 6001, clock);
        hb2.heartbeatOnce();
        assertEquals(6001, (int) db.queryForObject(
                "SELECT forward_port FROM cluster_nodes WHERE node_id = 'A'", Integer.class));
    }

    @Test
    void heartbeatWithoutForwardPortAdvertisesZero() {
        HeartbeatService hb = new HeartbeatService(db, "A", "host", 5000, clock);
        hb.heartbeatOnce();
        assertEquals(0, (int) db.queryForObject(
                "SELECT forward_port FROM cluster_nodes WHERE node_id = 'A'", Integer.class));
    }

    @Test
    void gcPauseAboveThresholdTriggersImmediateHeartbeat() {
        HeartbeatService hb = new HeartbeatService(db, "A", "host", 5000, clock);

        hb.maybeHeartbeatForGcPause(ClusterConfig.GC_PAUSE_THRESHOLD + 1);
        assertEquals(clock.now(), heartbeatOf("A"));
    }

    @Test
    void shortGcPauseDoesNotHeartbeat() {
        HeartbeatService hb = new HeartbeatService(db, "A", "host", 5000, clock);

        hb.maybeHeartbeatForGcPause(ClusterConfig.GC_PAUSE_THRESHOLD - 1);
        assertNull(heartbeatOf("A"), "a sub-threshold pause must not upsert");
    }

    @Test
    void dbFailureInHeartbeatIsSwallowed() {
        Db failing = new DefaultDb(brokenDataSource());
        HeartbeatService hb = new HeartbeatService(failing, "A", "host", 5000, clock);

        assertDoesNotThrow(hb::heartbeatOnce);
        assertDoesNotThrow(() -> hb.maybeHeartbeatForGcPause(ClusterConfig.GC_PAUSE_THRESHOLD + 1));
    }

    @Test
    void gcListenerLifecycleDoesNotThrow() {
        HeartbeatService hb = new HeartbeatService(db, "A", "host", 5000, clock);
        assertDoesNotThrow(hb::start);
        assertDoesNotThrow(hb::stop);
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
