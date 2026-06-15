package org.kendar.cqrses.cluster;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The lease-renewal step batches every held partition into a single
 * {@code UPDATE ... WHERE lease_holder = ? AND item_id IN (...)} per tick.
 * Semantics must stay identical to the former one-UPDATE-per-partition loop:
 * all held leases advance, and a row whose lease belongs to someone else (or
 * was cleared) is untouched.
 */
class WorkerServiceLeaseBatchTest extends ClusterTestBase {

    private void putAssignment(int item, String owner, long epoch) {
        db.insertInto("cluster_assignments")
                .set("item_id", item).set("owner_node", owner).set("epoch", epoch).execute();
    }

    private Long leaseUntil(int item) {
        return db.queryForObject(
                "SELECT lease_until FROM cluster_assignments WHERE item_id = ?", Long.class, item);
    }

    private String leaseHolder(int item) {
        return db.queryForObject(
                "SELECT lease_holder FROM cluster_assignments WHERE item_id = ?", String.class, item);
    }

    @Test
    void renewsAllHeldLeasesInOneTick() {
        RecordingProcessor proc = new RecordingProcessor();
        WorkerService a = new WorkerService(db, "A", clock, proc);

        putAssignment(0, "A", 1);
        putAssignment(1, "A", 1);
        putAssignment(2, "A", 1);

        // First tick: A claims all three (lease_until = now + LEASE).
        a.workerTick();
        assertTrue(proc.awaitProcessing(0, 1000));
        assertTrue(proc.awaitProcessing(1, 1000));
        assertTrue(proc.awaitProcessing(2, 1000));
        long firstLease = clock.now() + ClusterConfig.LEASE;
        for (int item = 0; item < 3; item++) {
            assertEquals("A", leaseHolder(item));
            assertEquals(firstLease, leaseUntil(item));
        }

        // Advance time and tick again: the batched renewal must move every
        // held lease forward to the new now + LEASE.
        clock.advance(1_000);
        a.workerTick();
        long renewed = clock.now() + ClusterConfig.LEASE;
        for (int item = 0; item < 3; item++) {
            assertEquals(renewed, leaseUntil(item), "lease for partition " + item + " not renewed");
        }

        a.stop();
    }

    @Test
    void doesNotTouchLeasesHeldByAnotherNode() {
        RecordingProcessor proc = new RecordingProcessor();
        WorkerService a = new WorkerService(db, "A", clock, proc);

        putAssignment(0, "A", 1);
        // Partition 1 is owned by A on paper but its lease is still held by B
        // (wind-down in progress on the other node).
        putAssignment(1, "A", 1);
        long bLease = clock.now() + ClusterConfig.LEASE;
        db.update("UPDATE cluster_assignments SET lease_holder = ?, lease_until = ? WHERE item_id = ?",
                "B", bLease, 1);

        a.workerTick();
        assertTrue(proc.awaitProcessing(0, 1000));

        // A renews only its own lease; B's row is untouched by the batched UPDATE.
        clock.advance(1_000);
        a.workerTick();
        assertEquals(clock.now() + ClusterConfig.LEASE, leaseUntil(0));
        assertEquals("B", leaseHolder(1));
        assertEquals(bLease, leaseUntil(1), "another node's lease must not be renewed");

        a.stop();
    }

    @Test
    void emptyActiveSetIssuesNoRenewal() {
        RecordingProcessor proc = new RecordingProcessor();
        WorkerService a = new WorkerService(db, "A", clock, proc);

        // No assignments at all: tick must not throw and must not invent rows.
        a.workerTick();
        assertEquals(0L, db.queryForObject(
                "SELECT COUNT(*) FROM cluster_assignments", Long.class));

        a.stop();
    }
}
