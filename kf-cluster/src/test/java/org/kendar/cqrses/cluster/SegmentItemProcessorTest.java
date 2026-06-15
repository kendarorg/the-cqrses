package org.kendar.cqrses.cluster;

import org.junit.jupiter.api.Test;
import org.kendar.cqrses.cluster.spi.SegmentOwnership;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The adapter is a pure delegation: {@code process → claimSegment}, and
 * {@code stopProcess → releaseSegment(seg, () -> node.release(seg))}. Verifies the
 * drained-callback fires {@code ClusterNode.release} so the cooperative
 * {@code stopProcess → release} lifecycle is wired correctly.
 */
class SegmentItemProcessorTest {

    private static final class FakeOwnership implements SegmentOwnership {
        final List<Integer> claimed = new ArrayList<>();
        final List<Integer> released = new ArrayList<>();

        @Override
        public void claimSegment(int segment) {
            claimed.add(segment);
        }

        @Override
        public void releaseSegment(int segment, Runnable onDrained) {
            released.add(segment);
            // The real engine runs onDrained only after draining; here run it inline
            // to assert it leads to ClusterNode.release.
            onDrained.run();
        }
    }

    private static final class RecordingNode extends ClusterNode {
        final List<Integer> releases = new ArrayList<>();

        RecordingNode() {
            super(null, "test-node", null, null, null, 0);
        }

        @Override
        public void release(int itemId) {
            releases.add(itemId);
        }
    }

    @Test
    void process_claimsSegment() {
        FakeOwnership ownership = new FakeOwnership();
        RecordingNode node = new RecordingNode();
        SegmentItemProcessor p = new SegmentItemProcessor(ownership, node);

        p.process(2);

        assertEquals(List.of(2), ownership.claimed);
    }

    @Test
    void stopProcess_releasesSegmentThenReleasesLease() {
        FakeOwnership ownership = new FakeOwnership();
        RecordingNode node = new RecordingNode();
        SegmentItemProcessor p = new SegmentItemProcessor(ownership, node);

        p.stopProcess(1);

        assertEquals(List.of(1), ownership.released, "must wind the segment down");
        assertEquals(List.of(1), node.releases, "onDrained must clear the cluster lease");
    }
}
