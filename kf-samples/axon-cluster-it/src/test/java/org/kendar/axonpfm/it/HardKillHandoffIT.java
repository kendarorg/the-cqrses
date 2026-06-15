package org.kendar.axonpfm.it;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Contrast to {@link ApiStopRestartHandoffIT}: hard-kill a node's container (no graceful processor
 * shutdown, no claim release) and assert the same outcome — its segments are stolen by the survivors
 * once its token claims time out, and the shared read model stays correct. Proves crash recovery
 * (claims simply expire), not just a graceful leave. Budget covers the full claim timeout
 * ({@link #CLAIM_TIMEOUT_MS}) plus a couple of coordination cycles.
 */
class HardKillHandoffIT extends AbstractClusterIT {

    @Test
    void hardKillReassignsSegmentsAndKeepsReadModelCorrect() {
        // 0. Cluster formed AND segments balanced across all three nodes.
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

        // 3. node3's claims expire and its segments are stolen by node1/node2.
        await().atMost(90, SECONDS).pollInterval(3, SECONDS).untilAsserted(() -> {
            assertThat(nodeLive("node3")).isFalse();
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
