package org.kendar.cqrses.cluster;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DbLeaderLockTest extends ClusterTestBase {

    @Test
    void exactlyOneAcquiresAndEpochIncrementsMonotonically() {
        DbLeaderLock a = new DbLeaderLock(db, "A", clock);
        DbLeaderLock b = new DbLeaderLock(db, "B", clock);

        long ea = a.acquire();
        long eb = b.acquire();

        // Exactly one wins; the loser gets -1.
        assertTrue(ea > 0);
        assertEquals(-1, eb);
        assertTrue(a.isHeld());
        assertFalse(b.isHeld());

        // The holder renewing bumps the epoch monotonically.
        long ea2 = a.acquire();
        assertTrue(ea2 > ea);
    }

    @Test
    void expiredLockIsReclaimableAndOldHolderNoLongerHeld() {
        DbLeaderLock a = new DbLeaderLock(db, "A", clock);
        DbLeaderLock b = new DbLeaderLock(db, "B", clock);

        long ea = a.acquire();
        assertTrue(a.isHeld());

        // Let A's lock-lease expire without renewal.
        clock.advance(ClusterConfig.LEADER_LOCK_LEASE + 1);
        assertFalse(a.isHeld());

        long eb = b.acquire();
        assertTrue(eb > ea); // strictly monotonic across the handover
        assertTrue(b.isHeld());
        assertFalse(a.isHeld());
    }

    @Test
    void releaseFreesTheLock() {
        DbLeaderLock a = new DbLeaderLock(db, "A", clock);
        a.acquire();
        a.release();
        assertFalse(a.isHeld());

        DbLeaderLock b = new DbLeaderLock(db, "B", clock);
        assertTrue(b.acquire() > 0);
    }
}
