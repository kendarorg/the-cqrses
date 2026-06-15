package org.kendar.axonpfm.it;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Drive a node's cluster membership <b>through the control API</b> (not a container kill), so the JVM
 * / ports / debugger stay up the whole time. Axon analog of the kf test: {@code /cluster/stop} shuts
 * node1's streaming processors down (releasing its token claims → node2/node3 steal them);
 * {@code /cluster/start} restarts them (reclaiming some). The read model stays correct throughout
 * (idempotent under at-least-once token replay).
 */
class ApiStopRestartHandoffIT extends AbstractClusterIT {

    @Test
    void stopAndRestartNodeViaApiHandsOffSegmentsAndKeepsReadModelCorrect() {
        // 0. Cluster formed: all 6 segments owned, distributed.
        await().atMost(60, SECONDS).pollInterval(2, SECONDS).untilAsserted(() -> {
            assertThat(liveNodeCount()).isEqualTo(NODE_COUNT);
            assertThat(segmentOwners().values()).doesNotContainNull().hasSize(SEGMENTS);
        });

        // 1. Drive load (write via node1) and let the shared read model converge (read via node2).
        login(0, "alice");
        recordOp(0, "alice", "IN", 1000, "salary");
        recordOp(0, "alice", "OUT", 300, "rent");
        await().atMost(30, SECONDS).pollInterval(1, SECONDS)
                .until(() -> summaryNet(1, "alice") == 700L);

        assertThat(ownedBy("node1")).as("node1 owns some segments before stop").isNotEmpty();

        // 2. Stop node1's cluster part via the API.
        String stopped = clusterStop(0);
        assertThat(jsonBool(stopped, "enabled")).isTrue();
        assertThat(jsonBool(stopped, "running")).isFalse();
        // JVM/ports/HTTP surface stay up: status still answers, reads still work.
        assertThat(clusterRunning(0)).isFalse();
        assertThat(summaryNet(0, "alice")).isEqualTo(700L);

        // 3. node1's claims are released and stolen by node2/node3.
        await().atMost(90, SECONDS).pollInterval(2, SECONDS).untilAsserted(() -> {
            Map<Integer, String> owners = segmentOwners();
            assertThat(owners.values()).doesNotContainNull().hasSize(SEGMENTS);
            assertThat(owners.values()).doesNotContain("node1");
            assertThat(Set.copyOf(owners.values())).containsExactlyInAnyOrder("node2", "node3");
        });

        // 4. Keep driving load through node2; the read model stays correct under the handoff.
        recordOp(1, "alice", "IN", 200, "bonus");
        await().atMost(30, SECONDS).pollInterval(1, SECONDS)
                .until(() -> summaryNet(2, "alice") == 900L);

        // 5. Restart node1's cluster part → it rejoins and steals some segments back.
        String started = clusterStart(0);
        assertThat(jsonBool(started, "running")).isTrue();
        await().atMost(90, SECONDS).pollInterval(2, SECONDS).untilAsserted(() -> {
            assertThat(clusterRunning(0)).isTrue();
            assertThat(liveNodeCount()).isEqualTo(NODE_COUNT);
            assertThat(segmentOwners().values()).contains("node1");
        });

        // 6. Final correctness check after rejoin.
        recordOp(0, "alice", "OUT", 100, "coffee");
        await().atMost(30, SECONDS).pollInterval(1, SECONDS)
                .until(() -> summaryNet(1, "alice") == 800L);
    }

    private Set<Integer> ownedBy(String node) {
        return segmentOwners().entrySet().stream()
                .filter(e -> node.equals(e.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }
}
