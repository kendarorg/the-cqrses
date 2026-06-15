package org.kendar.cqrses.cluster;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The no-double-pump core (verification #5): a partition handed off A→B must never have two active
 * pumps. B's claim CAS is blocked for the whole app-paced wind-down because A keeps renewing its
 * lease; only after the app on A calls {@code release} does B claim.
 */
class WorkerHandoffTest extends ClusterTestBase {

    private void putAssignment(int item, String owner, long epoch) {
        db.insertInto("cluster_assignments")
                .set("item_id", item).set("owner_node", owner).set("epoch", epoch).execute();
    }

    private void setOwner(int item, String owner) {
        db.update("UPDATE cluster_assignments SET owner_node = ? WHERE item_id = ?", owner, item);
    }

    @Test
    void noIntervalWithTwoActivePumpsAcrossHandoff() {
        RecordingProcessor procA = new RecordingProcessor();
        RecordingProcessor procB = new RecordingProcessor();
        WorkerService a = new WorkerService(db, "A", clock, procA);
        WorkerService b = new WorkerService(db, "B", clock, procB);

        putAssignment(0, "A", 1);

        // A claims and starts pumping partition 0.
        a.workerTick();
        assertTrue(procA.awaitProcessing(0, 1000));
        assertFalse(procB.isProcessing(0));

        // Leader flips ownership to B.
        setOwner(0, "B");

        // B cannot claim — A's lease is still live (renewed, keyed on lease_holder=A).
        b.workerTick();
        assertFalse(procB.isProcessing(0), "B must not pump while A still holds the lease");

        // A notices the loss, asks the app to wind down, but keeps the lease until release.
        a.workerTick();
        assertTrue(procA.stopRequested(0));
        // A's pump has wound down (process returned), yet the lease is still A's.
        assertTrue(RecordingProcessor.await(() -> !procA.isProcessing(0), 1000));

        // B still cannot claim — lease not yet released.
        b.workerTick();
        assertFalse(procB.isProcessing(0));
        assertEquals(0, procB.processStarts());

        // App on A finishes the wind-down → release clears the lease.
        a.release(0);

        // Now B claims and starts the one and only second pump.
        b.workerTick();
        assertTrue(procB.awaitProcessing(0, 1000));
        assertFalse(procA.isProcessing(0));
        assertEquals(1, procB.processStarts());

        a.stop();
        b.stop();
    }

    @Test
    void appThatNeverReleasesStallsHandoffWithNoForceClear() {
        RecordingProcessor procA = new RecordingProcessor();
        RecordingProcessor procB = new RecordingProcessor();
        WorkerService a = new WorkerService(db, "A", clock, procA);
        WorkerService b = new WorkerService(db, "B", clock, procB);

        putAssignment(0, "A", 1);
        a.workerTick();
        assertTrue(procA.awaitProcessing(0, 1000));

        setOwner(0, "B");
        a.workerTick();            // stopProcess + keep renewing
        assertTrue(procA.stopRequested(0));

        // A never calls release — B is starved across many ticks (stall, not force-clear).
        for (int i = 0; i < 5; i++) {
            a.workerTick();        // A keeps renewing the lease
            b.workerTick();        // B keeps failing to claim
        }
        assertFalse(procB.isProcessing(0));
        assertEquals(0, procB.processStarts());

        a.stop();
        b.stop();
    }
}
