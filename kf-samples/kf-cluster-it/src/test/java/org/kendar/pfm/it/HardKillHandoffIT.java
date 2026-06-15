package org.kendar.pfm.it;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Contrast to {@link ApiStopRestartHandoffIT}: instead of a graceful API stop, hard-kill a node's
 * container (no warning, no leader-lock release, no graceful pump drain) and assert the same outcome
 * — its segments reassign to the survivors and the shared read model stays correct. This proves
 * crash recovery (heartbeat simply goes stale), not just a graceful leave.
 *
 * <p>Optional / slower: crash recovery waits out the full lease ({@code LEASE} 30s) on top of
 * staleness + stabilize, so budget ~120s for the reassignment.
 */
class HardKillHandoffIT extends AbstractClusterIT {

    @Test
    void hardKillReassignsSegmentsAndKeepsReadModelCorrect() {
        // 0. Cluster formed AND segments balanced across all three nodes. A non-null owner for every
        // segment is not enough: the first node up claims all SEGMENTS before its peers join, and the
        // leader only rebalances to a 2/2/2 spread after MEMBERSHIP_STABILIZE of stable 3-node
        // membership. Wait for that spread, otherwise the node3-owns-segments precondition below races
        // the rebalance.
        await().atMost(60, SECONDS).pollInterval(2, SECONDS).untilAsserted(() -> {
            Map<Integer, String> owners = segmentOwners();
            assertThat(owners.values()).doesNotContainNull().hasSize(SEGMENTS);
            assertThat(Set.copyOf(owners.values())).as("segments spread across all nodes")
                    .containsExactlyInAnyOrder(NODE_IDS);
        });

        // 1. Drive load and converge.
        login(0, "bob");
        recordOp(0, "bob", "IN", 500, "salary");
        await().atMost(30, SECONDS).pollInterval(1, SECONDS)
                .until(() -> summaryNet(0, "bob") == 500L);

        assertThat(ownedBy("node3")).as("node3 owns segments before kill").isNotEmpty();

        // 2. Hard kill node3 — no graceful /cluster/stop, just stop the container.
        stopNodeContainer(2);

        // 3. node3's heartbeat goes stale and its lease expires → segments reassign to node1/node2.
        await().atMost(120, SECONDS).pollInterval(3, SECONDS).untilAsserted(() -> {
            assertThat(heartbeatFresh("node3", STALENESS_WINDOW_MS)).isFalse();
            Map<Integer, String> owners = segmentOwners();
            assertThat(owners.values()).doesNotContainNull().hasSize(SEGMENTS);
            assertThat(owners.values()).doesNotContain("node3");
            assertThat(Set.copyOf(owners.values())).containsExactlyInAnyOrder("node1", "node2");
        });

        // 4. The two survivors keep the read model correct.
        recordOp(0, "bob", "IN", 100, "bonus");
        await().atMost(30, SECONDS).pollInterval(1, SECONDS)
                .until(() -> summaryNet(1, "bob") == 600L);
    }

    private Set<Integer> ownedBy(String node) {
        return segmentOwners().entrySet().stream()
                .filter(e -> node.equals(e.getValue()))
                .map(Map.Entry::getKey)
                .collect(java.util.stream.Collectors.toSet());
    }
}
