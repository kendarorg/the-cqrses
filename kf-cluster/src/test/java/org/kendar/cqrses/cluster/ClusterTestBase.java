package org.kendar.cqrses.cluster;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.kendar.cqrses.db.ConnectionStorage;
import org.kendar.cqrses.db.Db;
import org.kendar.cqrses.db.DefaultDb;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base for kf-cluster tests: a fresh in-memory H2 ({@code MODE=MySQL}) per method with the cluster
 * schema applied, plus a controllable {@link ClusterClock} so time advances without sleeping.
 */
public abstract class ClusterTestBase {

    private static final AtomicInteger DB_SEQ = new AtomicInteger();

    protected Db db;
    protected MutableClock clock;

    @BeforeEach
    void baseSetUp() {
        // connection() binds a connection to the thread for the duration of a path and DefaultDb
        // never closes it (that lifecycle belongs to the path boundary). Drop any connection a
        // prior test method left bound to the shared JUnit thread, otherwise connection() reuses it
        // and this method runs against the previous method's DB instead of its fresh datasource —
        // ClusterSchema's CREATE TABLE IF NOT EXISTS hides it, so isolation is lost silently.
        ConnectionStorage.unbind();
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:kfcluster_" + DB_SEQ.incrementAndGet() + ";MODE=MySQL;DB_CLOSE_DELAY=-1");
        db = new DefaultDb(ds);
        ClusterSchema.init(db);
        clock = new MutableClock(1_000_000L);
    }

    /**
     * A second, schema-less in-memory database for tests that need to set up legacy
     * tables themselves (e.g. schema-migration tests). Unbinds the thread-bound
     * connection first — otherwise {@code connection()} would silently reuse the
     * connection of {@link #db} and the "fresh" Db would act on the wrong database.
     */
    protected Db freshDb() {
        ConnectionStorage.unbind();
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:kfcluster_" + DB_SEQ.incrementAndGet() + ";MODE=MySQL;DB_CLOSE_DELAY=-1");
        return new DefaultDb(ds);
    }

    /** Insert (or refresh) a {@code cluster_nodes} row with the given heartbeat timestamp. */
    protected void putNode(String nodeId, String host, int port, long heartbeat) {
        int updated = db.update(
                "UPDATE cluster_nodes SET host = ?, liveness_port = ?, last_heartbeat = ? WHERE node_id = ?",
                host, port, heartbeat, nodeId);
        if (updated == 0) {
            db.insertInto("cluster_nodes")
                    .set("node_id", nodeId).set("host", host)
                    .set("liveness_port", port).set("last_heartbeat", heartbeat)
                    .execute();
        }
    }

    protected String ownerOf(int itemId) {
        return db.queryForObject(
                "SELECT owner_node FROM cluster_assignments WHERE item_id = ?", String.class, itemId);
    }

    protected Long epochOf(int itemId) {
        return db.queryForObject(
                "SELECT epoch FROM cluster_assignments WHERE item_id = ?", Long.class, itemId);
    }

    /** A {@link ClusterClock} whose time the test sets/advances explicitly. */
    protected static final class MutableClock implements ClusterClock {
        private final AtomicLong now;

        MutableClock(long start) {
            this.now = new AtomicLong(start);
        }

        @Override
        public long now() {
            return now.get();
        }

        void advance(long millis) {
            now.addAndGet(millis);
        }

        void set(long millis) {
            now.set(millis);
        }
    }
}
