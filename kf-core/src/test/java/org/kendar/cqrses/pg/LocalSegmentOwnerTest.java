package org.kendar.cqrses.pg;

import org.junit.jupiter.api.Test;
import org.kendar.cqrses.cluster.spi.SegmentOwnership;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link LocalSegmentOwner} against a fake {@link SegmentOwnership}: it must claim
 * every segment {@code 0..N-1} on parked threads and, on stop, release them all
 * (running each {@code onDrained}) and let the claim threads exit.
 */
class LocalSegmentOwnerTest {

    /** Mirrors SegmentProcessor's contract: claim parks until release runs onDrained + unparks. */
    static final class FakeOwnership implements SegmentOwnership {
        final Set<Integer> claimed = ConcurrentHashMap.newKeySet();
        final Set<Integer> released = ConcurrentHashMap.newKeySet();
        final ConcurrentHashMap<Integer, CountDownLatch> parks = new ConcurrentHashMap<>();

        @Override
        public void claimSegment(int segment) {
            CountDownLatch park = new CountDownLatch(1);
            parks.put(segment, park);          // publish the park BEFORE marking claimed
            claimed.add(segment);
            try {
                park.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void releaseSegment(int segment, Runnable onDrained) {
            released.add(segment);
            if (onDrained != null) onDrained.run();
            CountDownLatch park = parks.get(segment);
            if (park != null) park.countDown();
        }
    }

    private static void awaitUntil(BooleanSupplier cond, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return;
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted");
            }
        }
        fail("condition not met within " + timeoutMs + "ms");
    }

    @Test
    void startClaimsEverySegment_stopReleasesAndJoins() {
        FakeOwnership own = new FakeOwnership();
        LocalSegmentOwner owner = new LocalSegmentOwner(own, 4);

        owner.start();
        awaitUntil(() -> own.claimed.size() == 4, 2000);
        assertEquals(Set.of(0, 1, 2, 3), own.claimed, "every segment must be claimed");

        owner.stop();
        assertEquals(Set.of(0, 1, 2, 3), own.released, "every segment must be released on stop");
        // The parked claim threads must have unblocked and exited.
        awaitUntil(() -> Thread.getAllStackTraces().keySet().stream()
                .noneMatch(t -> t.getName().startsWith("local-seg-owner-")), 2000);
    }

    @Test
    void startIsIdempotent() {
        FakeOwnership own = new FakeOwnership();
        LocalSegmentOwner owner = new LocalSegmentOwner(own, 3);

        owner.start();
        owner.start(); // second call is a no-op — no extra claims
        awaitUntil(() -> own.claimed.size() == 3, 2000);
        assertEquals(3, own.claimed.size());

        owner.stop();
    }

    @Test
    void stopWithoutStartIsNoOp() {
        LocalSegmentOwner owner = new LocalSegmentOwner(new FakeOwnership(), 3);
        assertDoesNotThrow(owner::stop);
    }
}
