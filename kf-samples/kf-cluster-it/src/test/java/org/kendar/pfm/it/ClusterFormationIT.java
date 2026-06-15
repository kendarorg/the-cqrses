package org.kendar.pfm.it;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Three nodes join and form a healthy cluster: each reports {@code running:true}, MySQL shows three
 * fresh heartbeats, exactly one leader holds the lock, and all six segments are owned — spread across
 * all three live nodes (6/3 ⇒ 2 each). Budget ~40s: {@code MEMBERSHIP_STABILIZE} (10s) plus a couple
 * of leader/worker ticks (5s) for the first balanced assignment to settle.
 */
class ClusterFormationIT extends AbstractClusterIT {

    @Test
    void threeNodesFormClusterAndOwnAllSegments() {
        // Every node's control API reports the cluster part is running.
        for (int i = 0; i < NODE_COUNT; i++) {
            assertThat(clusterRunning(i)).as("node %d running", i).isTrue();
        }

        await().atMost(45, SECONDS).pollInterval(2, SECONDS).untilAsserted(() -> {
            assertThat(freshHeartbeatCount()).as("fresh heartbeats").isEqualTo(NODE_COUNT);
            assertThat(currentLeader()).as("a leader holds the lock").isNotNull();

            Map<Integer, String> owners = segmentOwners();
            assertThat(owners.keySet()).as("all segments present")
                    .containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5);
            assertThat(owners.values()).as("every segment has an owner").doesNotContainNull();
            assertThat(Set.of(NODE_IDS)).as("owners are live nodes").containsAll(owners.values());
            assertThat(new HashSet<>(owners.values())).as("balanced across all 3 nodes")
                    .hasSize(NODE_COUNT);
        });
    }
}
