package org.kendar.pfm.it;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Command forwarding to the segment-owning node ({@code kf.cluster.forwarding.enabled=true},
 * this class only — the other scenarios run with forwarding off and must stay byte-for-byte
 * today's behaviour).
 *
 * <p>All client traffic goes to <b>one</b> node. With 6 segments balanced over 3 nodes that
 * node owns ~2 of 6, so for a spread of users most commands target segments owned elsewhere
 * and must cross the forwarding channel. Asserts:
 * <ul>
 *   <li>every user's ledger converges to the expected net — commands executed exactly once
 *       wherever they landed;</li>
 *   <li>the traffic-receiving node's {@code forwardedCount} is &gt; 0 (it really forwarded
 *       rather than executing everything locally);</li>
 *   <li>the peers' {@code forwardServedCount} sums &gt; 0 (they really executed for it).</li>
 * </ul>
 * The probability that every user hashes into the receiving node's own segments (which would
 * make the counters 0) is (2/6)^users ≈ 5e-8 for 15 users — negligible.
 */
class CommandForwardingIT extends AbstractClusterIT {
    static {
        //thrower();
        // Loaded before the base @BeforeAll builds the containers; cleared by its @AfterAll.
        EXTRA_NODE_ENV.put("KF_CLUSTER_FORWARDING_ENABLED", "true");
        // default port 8071 on every node — distinct container network namespaces
    }


    private static final int USERS = 15;
    private static final int ENTRY_NODE = 0;

    @Test
    void commandsSentToNonOwnerAreForwardedAndLedgerConverges() {
        // 1. Wait for a fully-formed, balanced cluster: all 6 segments owned by 3 live nodes.
        await().atMost(60, SECONDS).pollInterval(2, SECONDS).untilAsserted(() -> {
            Map<Integer, String> owners = segmentOwners();
            assertThat(owners.keySet()).containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5);
            assertThat(owners.values()).doesNotContainNull();
            assertThat(new HashSet<>(owners.values())).hasSize(NODE_COUNT);
        });

        // 2. All traffic through ENTRY_NODE: each user +100 IN, -40 OUT → net 60.
        List<String> users = java.util.stream.IntStream.range(0, USERS)
                .mapToObj(i -> "fwd-user-" + i).toList();
        for (String user : users) {
            login(ENTRY_NODE, user);
            recordOp(ENTRY_NODE, user, "IN", 100, "salary");
            recordOp(ENTRY_NODE, user, "OUT", 40, "rent");
        }
        event("traffic-sent");

        // 3. The ledger projection converges for every user — each command executed
        //    exactly once, on whichever node owned its segment.
        await().atMost(90, SECONDS).pollInterval(2, SECONDS).untilAsserted(() -> {
            for (String user : users) {
                assertThat(summaryNet(ENTRY_NODE, user)).as("net of %s", user).isEqualTo(60L);
            }
        });
        event("ledger-converged");

        // 4. The forwarding channel was genuinely exercised.
        long forwarded = jsonLong(clusterStatus(ENTRY_NODE), "forwardedCount");
        assertThat(forwarded).as("entry node forwarded to peers").isGreaterThan(0);

        long servedByPeers = 0;
        for (int i = 0; i < NODE_COUNT; i++) {
            if (i == ENTRY_NODE) continue;
            servedByPeers += jsonLong(clusterStatus(i), "forwardServedCount");
        }
        assertThat(servedByPeers).as("peers executed forwarded commands").isGreaterThan(0);

        // 5. Symmetry: nothing was lost between the two counters' viewpoints — the
        //    entry node never serves itself, and peers sent no traffic of their own.
        assertThat(servedByPeers).isEqualTo(forwarded);
    }
}
