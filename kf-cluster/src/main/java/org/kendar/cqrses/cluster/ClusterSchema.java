package org.kendar.cqrses.cluster;

import org.kendar.cqrses.db.Db;
import org.kendar.cqrses.db.DbException;

/**
 * Idempotent DDL + singleton-row seeding for the kf-cluster tables, plus the
 * {@code cluster_config.item_count} seed-and-validate used at node startup.
 * <p>
 * Every statement is in the H2 &cap; MySQL intersection ({@code MODE=MySQL}) so the identical
 * SQL runs against a real MySQL server. Time columns are {@code BIGINT} epoch millis.
 */
public final class ClusterSchema {

    private ClusterSchema() {
    }

    /**
     * Create every table {@code IF NOT EXISTS} and seed the singleton ({@code id = 1}) rows for
     * the leader-lock and leader-health tables. Idempotent: a second call is a no-op (the
     * {@code INSERT IGNORE} drops the duplicate singleton).
     */
    public static void init(Db db) {
        db.execute("""
                CREATE TABLE IF NOT EXISTS cluster_nodes (
                  node_id        VARCHAR(190) PRIMARY KEY,
                  host           VARCHAR(255) NOT NULL,
                  liveness_port  INT          NOT NULL,
                  forward_port   INT          NOT NULL DEFAULT 0,
                  last_heartbeat BIGINT       NOT NULL
                )""");
        migrateForwardPort(db);
        db.execute("""
                CREATE TABLE IF NOT EXISTS cluster_assignments (
                  item_id      INT          PRIMARY KEY,
                  owner_node   VARCHAR(190),
                  epoch        BIGINT       NOT NULL DEFAULT 0,
                  lease_holder VARCHAR(190),
                  lease_until  BIGINT
                )""");
        db.execute("""
                CREATE TABLE IF NOT EXISTS cluster_leader_health (
                  id        INT    PRIMARY KEY,
                  last_tick BIGINT NOT NULL,
                  epoch     BIGINT NOT NULL
                )""");
        db.execute("""
                CREATE TABLE IF NOT EXISTS cluster_leader_lock (
                  id          INT          PRIMARY KEY,
                  owner_node  VARCHAR(190),
                  epoch       BIGINT NOT NULL DEFAULT 0,
                  lease_until BIGINT NOT NULL DEFAULT 0
                )""");
        db.execute("""
                CREATE TABLE IF NOT EXISTS cluster_config (
                  id         INT PRIMARY KEY,
                  item_count INT NOT NULL
                )""");

        db.insertInto("cluster_leader_lock").set("id", 1).set("epoch", 0L).set("lease_until", 0L).ignore().execute();
        db.insertInto("cluster_leader_health").set("id", 1).set("last_tick", 0L).set("epoch", 0L).ignore().execute();
    }

    /**
     * Add {@code forward_port} to a {@code cluster_nodes} table created before command
     * forwarding existed. Probe-then-alter: neither {@code ADD COLUMN IF NOT EXISTS}
     * (missing in MySQL) nor an unconditional {@code ADD COLUMN} (fails when present)
     * is in the H2 &cap; MySQL intersection, but a zero-row probe SELECT and a plain
     * {@code ALTER TABLE} both are. {@code DEFAULT 0} = "node does not accept forwards",
     * which is also the mixed-version safety valve during a rolling upgrade.
     */
    private static void migrateForwardPort(Db db) {
        try {
            db.queryForObject("SELECT forward_port FROM cluster_nodes WHERE 1 = 0", Integer.class);
        } catch (DbException e) {
            db.execute("ALTER TABLE cluster_nodes ADD COLUMN forward_port INT NOT NULL DEFAULT 0");
        }
    }

    /**
     * Seed {@code cluster_config.item_count} once (first writer wins via {@code INSERT IGNORE})
     * and validate the local {@code itemCount} against the stored value. The same {@code N} is
     * supplied identically to every node; a node started with a different {@code N} than the seeded
     * value refuses to start.
     *
     * @throws IllegalStateException if the stored item count differs from {@code itemCount}
     */
    public static void seedAndValidateItemCount(Db db, int itemCount) {
        db.insertInto("cluster_config").set("id", 1).set("item_count", itemCount).ignore().execute();
        Integer stored = db.queryForObject(
                "SELECT item_count FROM cluster_config WHERE id = 1", Integer.class);
        if (stored == null || stored != itemCount) {
            throw new IllegalStateException(
                    "cluster item_count mismatch: stored=" + stored + " local=" + itemCount
                            + " — every node must start with the same N");
        }
    }
}
