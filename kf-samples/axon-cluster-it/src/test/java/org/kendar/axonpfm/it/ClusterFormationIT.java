package org.kendar.axonpfm.it;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Three nodes join and form a healthy cluster: each reports {@code running:true}, and all six
 * segments are claimed — spread across all three nodes (6/3 ⇒ 2 each). The Axon analog of the kf
 * formation test, minus the single-leader assertion: server-less Axon has no leader, work is
 * distributed by peer-to-peer token stealing, so balance is the only invariant. Budget covers the
 * initial pooled-processor segment claiming + a couple of coordination cycles to settle the spread.
 */
class ClusterFormationIT extends AbstractClusterIT {

    @Test
    void threeNodesFormClusterAndOwnAllSegments() {
        for (int i = 0; i < NODE_COUNT; i++) {
            assertThat(clusterRunning(i)).as("node %d running", i).isTrue();
        }

        await().atMost(60, SECONDS).pollInterval(2, SECONDS).untilAsserted(() -> {
            assertThat(liveNodeCount()).as("live nodes").isEqualTo(NODE_COUNT);

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
