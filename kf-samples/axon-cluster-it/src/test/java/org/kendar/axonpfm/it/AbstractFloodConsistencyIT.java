package org.kendar.axonpfm.it;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Shared endurance / consistency scenario (Axon port of the kf one): <b>flood the cluster with 100
 * users, take node3 out, keep hammering, put it back, then stop and prove the read model is exactly
 * right.</b> Subclasses decide <i>how</i> node3 leaves/rejoins (graceful {@code /cluster/stop} vs hard
 * container kill).
 *
 * <p>The bookkeeping ({@link LoadGenerator}) counts only acked ops, and node3 is pulled from write
 * rotation and the flood quiesced <i>before</i> take-down, so no in-flight write can be ambiguously
 * lost. What remains — Axon's at-least-once token replay on handoff — is absorbed by the idempotent
 * (insert-ignore on {@code op_id}) read model. A projection handler that throws transiently is routed
 * to the per-aggregate dead-letter queue rather than dropped, so before each consistency verdict this
 * base re-runs every dead letter at least once via {@code POST /dlq/retry-all}. Exact equality is then
 * provable. Runs for minutes; Docker-gated.
 */
abstract class AbstractFloodConsistencyIT extends AbstractClusterIT {

    protected static final int USER_COUNT = 100;
    protected static final int FLOOD_THREADS = 4;
    protected static final long FLOOD_THROTTLE_MS = 12L;

    protected static final long FLOOD_WARMUP_MS = 8_000L;
    protected static final long FLOOD_WHILE_DOWN_MS = 8_000L;
    protected static final long FLOOD_AFTER_RESTART_MS = 8_000L;

    protected static final long FINAL_DRAIN_MS = 60_000L;
    protected static final long DLQ_SETTLE_MS = 5_000L;

    protected static final int NODE3 = 2;

    private volatile LoadGenerator activeLoad;

    /** Run totals captured during the scenario so the consolidated TOTALS summary prints on a pass and
     *  a failure alike (the load generator itself is local to {@link #runFloodScenario()}). */
    private volatile long totalAckedOps = -1L;
    private volatile long totalGrandNet = 0L;
    private volatile long totalFailedOps = 0L;

    // The flood reports its LoadGenerator's own bookkeeping (captured at quiesce) rather than the
    // base harness's per-call counters, so the TOTALS reflect the exact acked/net asserted on.
    @Override
    protected long reportAckedOps() {
        return totalAckedOps >= 0 ? totalAckedOps : super.reportAckedOps();
    }

    @Override
    protected long reportGrandNet() {
        return totalGrandNet;
    }

    @Override
    protected long reportFailedOps() {
        return totalFailedOps;
    }

    @Override
    protected long reportExpectedUsers() {
        return USER_COUNT;
    }

    protected abstract void takeDownNode3();

    protected abstract void bringBackNode3();

    protected abstract String variant();

    @Test
    void floodTakeDownRestartAndVerifyEverythingConsistent() {
        try {
            runFloodScenario();
        } catch (AssertionError | RuntimeException failure) {
            dumpInconsistencyDiagnostics(failure);
            throw failure;
        }
        // The comparison-friendly TOTALS block and the markdown report (target/axon-metrics-<Class>.md,
        // with the test timeline + metrics) are emitted by the base @AfterEach, on pass and fail alike.
    }

    /** Title the report with the take-down variant ({@code api-stop} / {@code hard-kill}). */
    @Override
    protected String scenarioLabel(org.junit.jupiter.api.TestInfo info) {
        return variant();
    }

    private void runFloodScenario() {
        List<String> users = users(USER_COUNT);
        LOGGER.trace("=== flood consistency scenario, variant: {} ===", variant());

        // 0. Healthy, balanced 3-node cluster.
        await().atMost(60, SECONDS).pollInterval(2, SECONDS).untilAsserted(() -> {
            assertThat(liveNodeCount()).as("live nodes").isEqualTo(NODE_COUNT);
            Map<Integer, String> owners = segmentOwners();
            assertThat(owners.values()).as("all segments owned").doesNotContainNull().hasSize(SEGMENTS);
            assertThat(Set.copyOf(owners.values())).as("balanced across all 3 nodes")
                    .containsExactlyInAnyOrder(NODE_IDS);
        });

        // 1. Register all 100 users (round-robin across the live nodes).
        for (int i = 0; i < users.size(); i++) {
            login(i % NODE_COUNT, users.get(i));
        }
        LOGGER.trace("registered {} users", users.size());

        LoadGenerator load = new LoadGenerator(this::recordOp, users, FLOOD_THREADS, FLOOD_THROTTLE_MS);
        activeLoad = load;
        try {
            // 2. Flood across all three nodes.
            load.liveNodes.addAll(Set.of(0, 1, 2));
            load.start();
            floodFor(load, FLOOD_WARMUP_MS, "warmup");
            assertThat(load.ackedOps()).as("flood produced load").isPositive();
            // node3 demonstrably carries a share of the segments before we pull it. Ask node3 itself
            // (ownedSegmentsOf), not the collapsed segmentOwners() map: that map flattens the two
            // independent processors (ledger + users) into one segment→node entry per segment with
            // last-write-wins and needs every node reachable to build it, so it can hide node3's
            // claims. Polled because under the flood node3's /cluster/{status,segments} can briefly
            // time out (or read mid-coordinator-cycle), which ownedSegmentsOf() reports as empty.
            await().atMost(30, SECONDS).pollInterval(2, SECONDS).untilAsserted(() ->
                    assertThat(ownedSegmentsOf(NODE3)).as("node3 owns segments before take-down").isNotEmpty());

            // 3. Take node3 down (quiesced + out of write rotation first).
            load.liveNodes.remove(NODE3);
            load.pauseAndQuiesce();
            try {
                takeDownNode3();
            } finally {
                load.resume();
            }

            // node3's claims expire and its segments are stolen by the two survivors.
            await().atMost(120, SECONDS).pollInterval(3, SECONDS).untilAsserted(() -> {
                assertThat(nodeLive("node3")).isFalse();
                Map<Integer, String> owners = segmentOwners();
                assertThat(owners.values()).doesNotContainNull().hasSize(SEGMENTS);
                assertThat(Set.copyOf(owners.values())).containsExactlyInAnyOrder("node1", "node2");
            });

            // 4. Keep hammering the survivors, then quiesce and assert consistency.
            floodFor(load, FLOOD_WHILE_DOWN_MS, "node3 down");
            assertConsistentAfterQuiesce(load, "after node3 down", List.of(0, 1));

            // 5. Bring node3 back; it rejoins, steals segments back, re-enters rotation.
            bringBackNode3();
            await().atMost(120, SECONDS).pollInterval(3, SECONDS).untilAsserted(() -> {
                assertThat(clusterRunning(NODE3)).isTrue();
                assertThat(liveNodeCount()).isEqualTo(NODE_COUNT);
                assertThat(segmentOwners().values()).contains("node3");
            });
            load.liveNodes.add(NODE3);

            // 6. More load across all three now that node3 is back.
            floodFor(load, FLOOD_AFTER_RESTART_MS, "node3 back");
        } finally {
            // 7. Stop bombarding.
            load.stop();
        }

        long expectedOps = load.ackedOps();
        long expectedNet = load.grandNet();
        totalAckedOps = expectedOps;
        totalGrandNet = expectedNet;
        totalFailedOps = load.failedOps();
        LOGGER.trace("flood complete ({}): {} acked ops, grand net {}. Draining {}s before final check.",
                variant(), expectedOps, expectedNet, FINAL_DRAIN_MS / 1000);

        // 8. Wait the mandated 60s for the at-least-once pump to drain the last events.
        sleep(FINAL_DRAIN_MS);

        // 9. Drain the DLQ + converge, then assert exact totals.
        awaitConvergenceDrainingDlq(expectedOps, expectedNet, "final", List.of(0, 1, NODE3), 90);
        assertThat(dbUserCount()).as("all users registered").isEqualTo((long) USER_COUNT);

        // 10. Full per-user consistency from all three nodes.
        for (String user : users) {
            long expected = load.expectedNet(user);
            assertThat(summaryNet(0, user)).as("user %s net on node1", user).isEqualTo(expected);
            assertThat(summaryNet(1, user)).as("user %s net on node2", user).isEqualTo(expected);
            assertThat(summaryNet(NODE3, user)).as("user %s net on node3", user).isEqualTo(expected);
        }

        // 11. Cluster healthy and balanced again.
        await().atMost(60, SECONDS).pollInterval(2, SECONDS).untilAsserted(() -> {
            assertThat(liveNodeCount()).isEqualTo(NODE_COUNT);
            Map<Integer, String> owners = segmentOwners();
            assertThat(owners.values()).doesNotContainNull().hasSize(SEGMENTS);
            assertThat(Set.copyOf(owners.values())).containsExactlyInAnyOrder(NODE_IDS);
        });
    }

    private void assertConsistentAfterQuiesce(LoadGenerator load, String phase, List<Integer> readNodes) {
        load.pauseAndQuiesce();
        try {
            long ops = load.ackedOps();
            long net = load.grandNet();
            awaitConvergenceDrainingDlq(ops, net, phase, readNodes, 120);
            for (int u = 0; u < USER_COUNT; u += 10) {
                String user = userName(u);
                long expected = load.expectedNet(user);
                for (int node : readNodes) {
                    assertThat(summaryNet(node, user))
                            .as("user %s net on node%d (%s)", user, node + 1, phase)
                            .isEqualTo(expected);
                }
            }
        } finally {
            load.resume();
        }
    }

    private void awaitConvergenceDrainingDlq(long expectedOps, long expectedNet, String phase,
                                             List<Integer> liveNodes, long atMostSeconds) {
        LOGGER.trace("convergence [{}]: awaiting read model at {} ops / net {}", phase, expectedOps, expectedNet);
        long deadline = now() + atMostSeconds * 1000L;
        long count;
        long net;
        do {
            rerunAllDlq(liveNodes);
            sleep(DLQ_SETTLE_MS);
            count = dbOpCount();
            net = dbNet();
            LOGGER.trace("convergence [{}]: {}/{} ops projected (lag {}), net {}/{} (off by {})",
                    phase, count, expectedOps, expectedOps - count, net, expectedNet, expectedNet - net);
            if (count == expectedOps && net == expectedNet) {
                LOGGER.trace("convergence [{}]: reached {} ops / net {}", phase, expectedOps, expectedNet);
                return;
            }
        } while (now() < deadline);
        assertThat(count).as("ops projected (%s)", phase).isEqualTo(expectedOps);
        assertThat(net).as("grand net (%s)", phase).isEqualTo(expectedNet);
    }

    private void rerunAllDlq(List<Integer> liveNodes) {
        for (int node : liveNodes) {
            try {
                post(httpBase(node) + "/dlq/retry-all", null);
            } catch (RuntimeException e) {
                LOGGER.warn("dlq retry-all node{} failed: {}", node + 1, e.getMessage());
            }
        }
    }

    /**
     * On a consistency failure, dump the durable-vs-projected evidence. Axon tables:
     * {@code domain_event_entry} (durable events, by {@code payload_type}) and {@code dead_letter_entry}
     * (parked projection failures). Best-effort; never masks the original failure.
     */
    private void dumpInconsistencyDiagnostics(Throwable failure) {
        LOGGER.error("=== INCONSISTENCY DIAGNOSTICS (variant {}): {} ===", variant(), failure.getMessage());
        try (Connection c = db()) {
            dumpQuery(c, "durable OperationRecorded vs projected pfm_operation",
                    "SELECT (SELECT COUNT(*) FROM domain_event_entry WHERE payload_type LIKE '%OperationRecorded') AS durable_ops, "
                            + "(SELECT COUNT(*) FROM pfm_operation) AS projected_ops");
            dumpQuery(c, "durable UserRegistered vs projected pfm_user",
                    "SELECT (SELECT COUNT(*) FROM domain_event_entry WHERE payload_type LIKE '%UserRegistered') AS durable_users, "
                            + "(SELECT COUNT(*) FROM pfm_user) AS projected_users");
            dumpQuery(c, "dead_letter_entry by group (is anything parked?)",
                    "SELECT processing_group, COUNT(*) AS cnt FROM dead_letter_entry "
                            + "GROUP BY processing_group ORDER BY processing_group");
            dumpQuery(c, "token positions per processor/segment",
                    "SELECT processor_name, segment, owner FROM token_entry "
                            + "ORDER BY processor_name, segment");
        } catch (SQLException e) {
            LOGGER.warn("diagnostics: DB probe failed: {}", e.getMessage());
        }
        for (int node = 0; node < NODE_COUNT; node++) {
            try {
                LOGGER.error("diagnostics: /dlq/retry-all node{} → {}", node + 1,
                        post(httpBase(node) + "/dlq/retry-all", null));
            } catch (RuntimeException e) {
                LOGGER.warn("diagnostics: retry-all on node{} unreachable: {}", node + 1, e.getMessage());
            }
        }
    }

    private void dumpQuery(Connection c, String label, String sql) {
        LOGGER.error("diagnostics: {}", label);
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            boolean any = false;
            while (rs.next()) {
                any = true;
                StringBuilder sb = new StringBuilder("    ");
                for (int i = 1; i <= cols; i++) {
                    if (i > 1) sb.append(", ");
                    sb.append(md.getColumnLabel(i)).append('=').append(rs.getString(i));
                }
                LOGGER.error("{}", sb);
            }
            if (!any) LOGGER.error("    (no rows)");
        } catch (SQLException e) {
            LOGGER.warn("    query failed: {}", e.getMessage());
        }
    }

    private static List<String> users(int n) {
        List<String> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(userName(i));
        }
        return out;
    }

    private static String userName(int i) {
        return "flood-user-" + String.format("%03d", i);
    }

    private void floodFor(LoadGenerator load, long ms, String phase) {
        long deadline = now() + ms;
        long lastOps = load.ackedOps();
        long lastTick = now();
        LOGGER.trace("flood [{}]: {}ms window, starting at {} acked ops (live nodes {})",
                phase, ms, lastOps, load.liveNodes);
        long remaining;
        while ((remaining = deadline - now()) > 0) {
            sleep(Math.min(2_000L, remaining));
            long t = now();
            long ops = load.ackedOps();
            long dt = t - lastTick;
            long rate = dt > 0 ? (ops - lastOps) * 1000 / dt : 0;
            LOGGER.trace("flood [{}]: {} acked ops (+{} ≈ {}/s), grand net {}",
                    phase, ops, ops - lastOps, rate, load.grandNet());
            lastOps = ops;
            lastTick = t;
        }
        captureMetrics(phase, ms);
    }

    protected static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
