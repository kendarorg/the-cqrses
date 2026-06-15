package org.kendar.cqrses.cluster;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClusterSchemaTest extends ClusterTestBase {

    @Test
    void initIsIdempotentAndSeedsSingletonsOnce() {
        // baseSetUp already called init once; a second call must be a no-op.
        assertDoesNotThrow(() -> ClusterSchema.init(db));
        ClusterSchema.init(db);

        assertEquals(1, (long) db.queryForObject(
                "SELECT COUNT(*) FROM cluster_leader_lock", Long.class));
        assertEquals(1, (long) db.queryForObject(
                "SELECT COUNT(*) FROM cluster_leader_health", Long.class));
    }

    @Test
    void forwardPortColumnPresentWithZeroDefault() {
        db.insertInto("cluster_nodes")
                .set("node_id", "A").set("host", "h").set("liveness_port", 1)
                .set("last_heartbeat", 0L).execute();
        assertEquals(0, (int) db.queryForObject(
                "SELECT forward_port FROM cluster_nodes WHERE node_id = 'A'", Integer.class));
    }

    @Test
    void initMigratesPreForwardingNodesTable() {
        // Simulate a DB created before forward_port existed: old 4-column table.
        org.kendar.cqrses.db.Db fresh = freshDb();
        fresh.execute("""
                CREATE TABLE cluster_nodes (
                  node_id        VARCHAR(190) PRIMARY KEY,
                  host           VARCHAR(255) NOT NULL,
                  liveness_port  INT          NOT NULL,
                  last_heartbeat BIGINT       NOT NULL
                )""");
        fresh.insertInto("cluster_nodes")
                .set("node_id", "OLD").set("host", "h").set("liveness_port", 1)
                .set("last_heartbeat", 42L).execute();

        ClusterSchema.init(fresh);

        assertEquals(0, (int) fresh.queryForObject(
                "SELECT forward_port FROM cluster_nodes WHERE node_id = 'OLD'", Integer.class));
        assertEquals(42L, (long) fresh.queryForObject(
                "SELECT last_heartbeat FROM cluster_nodes WHERE node_id = 'OLD'", Long.class));
    }

    @Test
    void itemCountSeededOnceAndValidated() {
        ClusterSchema.seedAndValidateItemCount(db, 8);
        // Same N → fine, even though INSERT IGNORE drops the duplicate seed.
        assertDoesNotThrow(() -> ClusterSchema.seedAndValidateItemCount(db, 8));
        assertEquals(8, (int) db.queryForObject(
                "SELECT item_count FROM cluster_config WHERE id = 1", Integer.class));
    }

    @Test
    void nodeWithDifferentNRefusesToStart() {
        ClusterSchema.seedAndValidateItemCount(db, 8);
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> ClusterSchema.seedAndValidateItemCount(db, 16));
        org.junit.jupiter.api.Assertions.assertTrue(ex.getMessage().contains("item_count mismatch"));
    }
}
