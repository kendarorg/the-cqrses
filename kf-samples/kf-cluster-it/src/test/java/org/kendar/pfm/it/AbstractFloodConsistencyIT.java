package org.kendar.pfm.it;

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
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Shared endurance / consistency scenario: <b>flood the cluster with 100 users, take node3 out, keep
 * hammering, put it back, then stop and prove the read model is exactly right.</b> Concrete subclasses
 * only decide <i>how</i> node3 leaves and rejoins:
 * <ul>
 *   <li>{@link FloodStopRestartConsistencyIT} — graceful {@code /cluster/stop} + {@code /cluster/start}
 *       (JVM stays up).</li>
 *   <li>{@link FloodHardKillRestartConsistencyIT} — {@code container.stop()} crash + fresh
 *       {@code container.start()} (a brand-new JVM rejoins under the same node id).</li>
 * </ul>
 *
 * <h2>Why the totals stay exact even across a crash</h2>
 * The bookkeeping ({@link LoadGenerator}) counts only acked ops, so a write must be known-landed to be
 * expected. A <b>hard kill mid-write</b> would break that: node3 could persist the event to the shared
 * store yet the client sees a connection reset and never counts it, leaving {@code dbOpCount} forever
 * above the expected total — a real, un-provable inconsistency. So before <i>either</i> kind of
 * take-down this base pulls node3 out of write rotation and {@link LoadGenerator#pauseAndQuiesce()
 * quiesces} the flood, guaranteeing no in-flight write can be ambiguously lost. What remains — the
 * at-least-once re-dispatch when a segment hands off mid-event — is absorbed by the idempotent
 * (insert-ignore on a fresh server-side {@code opId}) read model and drained by the mandated 60s wait.
 *
 * <p>One more source of drift: a projection handler can throw transiently under the flood + rebalance
 * (e.g. a row-lock timeout on the shared read table). The app routes such failures to a per-aggregate
 * DLQ rather than dropping them, but the cluster pump still advances its checkpoint past them, so a
 * dead letter never re-applies on its own. So before each consistency verdict this base re-runs every
 * DLQ item at least once (idempotent) via {@code POST /dlq/retry-all} on the live nodes and waits
 * {@value #DLQ_SETTLE_MS}ms for the re-applies to settle. Exact equality is then provable.
 *
 * <p>Runs for several minutes; Docker-gated like its siblings. Abstract → JUnit never runs it directly.
 */
abstract class AbstractFloodConsistencyIT extends AbstractClusterIT {

    protected static final int USER_COUNT = 100;
    // 24 workers, not 4: with ~4 in-flight commands the flood was a closed loop bound by
    // per-command latency (~28 cmd/s regardless of server capacity), which both under-reports
    // throughput and never exercises the append coalescer's group commit. 24 keeps enough
    // concurrent senders per segment for real batches while staying well under MAX_BATCH.
    protected static final int FLOOD_THREADS = 24;
    protected static final long FLOOD_THROTTLE_MS = 12L;

    /** How long to keep the flood running in each load window. */
    protected static final long FLOOD_WARMUP_MS = 8_000L;
    protected static final long FLOOD_WHILE_DOWN_MS = 8_000L;
    protected static final long FLOOD_AFTER_RESTART_MS = 8_000L;

    /** The mandated post-flood drain before the final consistency verdict. */
    protected static final long FINAL_DRAIN_MS = 60_000L;

    /** Settle window after re-running the projection DLQ, for the idempotent re-applies to land. */
    protected static final long DLQ_SETTLE_MS = 5_000L;

    protected static final int NODE3 = 2; // the node we take down and bring back

    /** DIAGNOSTIC: the active load generator, so the on-failure dump can read its acked-opId set. */
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

    /**
     * Take node3 down. Called <b>while the flood is quiesced and node3 is out of write rotation</b>, so
     * an implementation may crash it without risking an ambiguous in-flight write.
     */
    protected abstract void takeDownNode3();

    /** Bring node3 back. Must return only once node3's HTTP/control surface answers again. */
    protected abstract void bringBackNode3();

    /** Short label for logs/assertions, e.g. {@code "api-stop"} or {@code "hard-kill"}. */
    protected abstract String variant();

    @Test
    void floodTakeDownRestartAndVerifyEverythingConsistent() {
        try {
            runFloodScenario();
        } catch (AssertionError | RuntimeException failure) {
            // Consistency failed (convergence lag, per-user mismatch, …). Dump the evidence
            // that tells the two silent-undercount root causes apart BEFORE rethrowing, so the
            // failing run itself shows whether an acked event was skipped at dispatch (no
            // dlq_item row, yet the checkpoint already advanced past it) or parked in the DLQ
            // and never re-applied. Best-effort: never masks the original failure.
            dumpInconsistencyDiagnostics(failure);
            throw failure;
        }
        // The comparison-friendly TOTALS block and the markdown report (target/kf-metrics-<Class>.md,
        // with the test timeline + metrics) are emitted by the base @AfterEach, on pass and fail alike.
    }

    /** Title the report with the take-down variant ({@code api-stop} / {@code hard-kill}). */
    @Override
    protected String scenarioLabel(org.junit.jupiter.api.TestInfo info) {
        return variant();
    }

    private void runFloodScenario() {
        List<String> users = users(USER_COUNT);
        LOGGER.info("=== flood consistency scenario, variant: {} ===", variant());

        // 0. Healthy, balanced 3-node cluster: every segment owned, spread across all three nodes.
        await().atMost(60, SECONDS).pollInterval(2, SECONDS).untilAsserted(() -> {
            assertThat(freshHeartbeatCount()).as("fresh heartbeats").isEqualTo(NODE_COUNT);
            Map<Integer, String> owners = segmentOwners();
            assertThat(owners.values()).as("all segments owned").doesNotContainNull().hasSize(SEGMENTS);
            assertThat(Set.copyOf(owners.values())).as("balanced across all 3 nodes")
                    .containsExactlyInAnyOrder(NODE_IDS);
        });

        // 1. Register all 100 users (round-robin across the live nodes).
        for (int i = 0; i < users.size(); i++) {
            login(i % NODE_COUNT, users.get(i));
        }
        LOGGER.info("registered {} users", users.size());

        LoadGenerator load = new LoadGenerator(this::recordOp, users, FLOOD_THREADS, FLOOD_THROTTLE_MS);
        activeLoad = load; // DIAGNOSTIC: let dumpInconsistencyDiagnostics read the acked-opId set.
        try {
            // 2. Flood across all three nodes.
            load.liveNodes.addAll(Set.of(0, 1, 2));
            load.start();
            floodFor(load, FLOOD_WARMUP_MS, "warmup");
            assertThat(load.ackedOps()).as("flood produced load").isPositive();
            assertThat(ownedBy("node3")).as("node3 owns segments before take-down").isNotEmpty();

            // 3. Take node3 down. Pull it from write rotation and quiesce first, so no in-flight write
            // can be ambiguously lost (critical for the hard-kill variant — see class javadoc).
            load.liveNodes.remove(NODE3);
            load.pauseAndQuiesce();
            try {
                takeDownNode3();
            } finally {
                load.resume();
            }

            // node3's heartbeat goes stale and its segments reassign to the two survivors. Budget for
            // the worst case (crash): full lease expiry + staleness + membership stabilize + ticks.
            await().atMost(120, SECONDS).pollInterval(3, SECONDS).untilAsserted(() -> {
                assertThat(heartbeatFresh("node3", STALENESS_WINDOW_MS)).isFalse();
                Map<Integer, String> owners = segmentOwners();
                assertThat(owners.values()).doesNotContainNull().hasSize(SEGMENTS);
                assertThat(Set.copyOf(owners.values())).containsExactlyInAnyOrder("node1", "node2");
            });

            // 4. Keep hammering the survivors, then quiesce and assert the read model is consistent.
            floodFor(load, FLOOD_WHILE_DOWN_MS, "node3 down");
            assertConsistentAfterQuiesce(load, "after node3 down", List.of(0, 1));

            // 5. Bring node3 back; it rejoins, rebalances, and re-enters rotation.
            bringBackNode3();
            await().atMost(120, SECONDS).pollInterval(3, SECONDS).untilAsserted(() -> {
                assertThat(clusterRunning(NODE3)).isTrue();
                assertThat(heartbeatFresh("node3", STALENESS_WINDOW_MS)).isTrue();
                assertThat(freshHeartbeatCount()).isEqualTo(NODE_COUNT);
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

        // 9. Rerun every dead-lettered projection event at least once, let the idempotent re-applies
        // settle, then assert the read model reflects exactly the acked totals.
        awaitConvergenceDrainingDlq(expectedOps, expectedNet, "final", List.of(0, 1, NODE3), 90);
        assertThat(dbUserCount()).as("all users registered").isEqualTo((long) USER_COUNT);

        // 10. Full per-user consistency: every user reads identically from ALL THREE nodes (incl. the
        // recovered node3) and equals its exact expected net.
        for (String user : users) {
            long expected = load.expectedNet(user);
            assertThat(summaryNet(0, user)).as("user %s net on node1", user).isEqualTo(expected);
            assertThat(summaryNet(1, user)).as("user %s net on node2", user).isEqualTo(expected);
            assertThat(summaryNet(NODE3, user)).as("user %s net on node3", user).isEqualTo(expected);
        }

        // 11. Cluster is healthy and balanced again.
        await().atMost(60, SECONDS).pollInterval(2, SECONDS).untilAsserted(() -> {
            assertThat(freshHeartbeatCount()).isEqualTo(NODE_COUNT);
            Map<Integer, String> owners = segmentOwners();
            assertThat(owners.values()).doesNotContainNull().hasSize(SEGMENTS);
            assertThat(Set.copyOf(owners.values())).containsExactlyInAnyOrder(NODE_IDS);
        });
    }

    /**
     * Quiesce the flood, wait for the read model to catch up to the exact acked totals, spot-check a
     * sample of users for agreement across the given read nodes, then resume.
     */
    private void assertConsistentAfterQuiesce(LoadGenerator load, String phase, List<Integer> readNodes) {
        load.pauseAndQuiesce();
        try {
            long ops = load.ackedOps();
            long net = load.grandNet();
            awaitConvergenceDrainingDlq(ops, net, phase, readNodes, 120);
            // A sample of users must read identically across the live read nodes, equal to expected.
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

    /**
     * Await the read model converging to exactly {@code expectedOps} rows and {@code expectedNet}
     * grand net, <b>re-running the projection DLQ before each check</b>.
     *
     * <p>The projection groups route a failed event to a per-aggregate DLQ rather than dropping it,
     * but the cluster pump still advances its checkpoint past it (at-least-once), so a dead letter
     * never re-applies on its own — it would strand the read model below the acked totals forever.
     * So each round we rerun every DLQ item at least once across the live nodes (idempotent,
     * insert-ignore on {@code op_id}), wait {@value #DLQ_SETTLE_MS}ms for the re-applies to land,
     * then read the totals. We repeat until they match or the budget runs out — a late dead letter
     * from the still-draining pump is picked up by the next round. The live lag is logged each round
     * (Awaitility is silent until timeout), which shows the read side catching up.
     */
    private void awaitConvergenceDrainingDlq(long expectedOps, long expectedNet, String phase,
                                             List<Integer> liveNodes, long atMostSeconds) {
        LOGGER.trace("convergence [{}]: awaiting read model at {} ops / net {}", phase, expectedOps, expectedNet);
        long deadline = now() + atMostSeconds * 1000L;
        long count = 0;
        long net = 0;
        do {
            rerunAllDlq(phase, liveNodes);
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

    /** Rerun every pending dead-lettered projection event at least once, on each live node in turn. */
    private void rerunAllDlq(String phase, List<Integer> liveNodes) {
        for (int node : liveNodes) {
            String body = post(httpBase(node) + "/dlq/retry-all", null);
            //LOGGER.trace("dlq retry-all [{}] node{}: {}", phase, node + 1, body);
        }
    }

    /**
     * On a consistency failure, dump the evidence that discriminates the two silent-undercount
     * root causes (both invisible to {@code failed} in the retry-all report):
     * <ol>
     *   <li><b>Skipped at dispatch.</b> The event was pumped from the segment tail but never
     *       dispatched to a handler (e.g. an unresolved event type), so it is in
     *       {@code event_entry} but <i>not</i> in {@code dlq_item}, and the projection
     *       {@code processor_checkpoint} has already advanced past it. Signature:
     *       {@code durable > projected}, {@code dlq_item} empty, {@code last_seq >= max_seq}.</li>
     *   <li><b>Parked but undrained.</b> The handler threw, the event sits in {@code dlq_item},
     *       but {@code /dlq/retry-all} never re-applies it (its status is one the retry query
     *       skips). Signature: {@code durable > projected} with rows left in {@code dlq_item}.</li>
     * </ol>
     * Self-contained and best-effort: every probe is guarded so a diagnostics error can never
     * replace the original assertion failure.
     */
    private void dumpInconsistencyDiagnostics(Throwable failure) {
        LOGGER.error("=== INCONSISTENCY DIAGNOSTICS (variant {}): {} ===", variant(), failure.getMessage());
        try (Connection c = db()) {
            dumpQuery(c, "durable OperationRecorded vs projected pfm_operation",
                    "SELECT (SELECT COUNT(*) FROM event_entry WHERE event_type='OperationRecorded') AS durable_ops, "
                            + "(SELECT COUNT(*) FROM pfm_operation) AS projected_ops");
            dumpQuery(c, "durable UserRegistered vs projected pfm_user",
                    "SELECT (SELECT COUNT(*) FROM event_entry WHERE event_type='UserRegistered') AS durable_users, "
                            + "(SELECT COUNT(*) FROM pfm_user) AS projected_users");
            dumpQuery(c, "dlq_item by group/status (is anything parked, and in what state?)",
                    "SELECT processing_group, status, COUNT(*) AS cnt FROM dlq_item "
                            + "GROUP BY processing_group, status ORDER BY processing_group, status");
            dumpQuery(c, "dlq_item detail (stuck items, FIFO order, last retry error)",
                    "SELECT processing_group, event_type, status, retry_count, error_class, last_retry_error_class "
                            + "FROM dlq_item ORDER BY sequence_id, ordinal");
            dumpQuery(c, "projection checkpoint vs segment tail "
                            + "(last_seq >= max_seq while a row is missing ⇒ skipped at dispatch)",
                    "SELECT pc.processing_group, pc.segment, pc.last_seq, sc.max_seq, sc.events "
                            + "FROM processor_checkpoint pc JOIN ("
                            + "SELECT segment, MAX(segment_seq) AS max_seq, COUNT(*) AS events "
                            + "FROM event_entry GROUP BY segment) sc ON sc.segment = pc.segment "
                            + "ORDER BY pc.processing_group, pc.segment");
        } catch (SQLException e) {
            LOGGER.warn("diagnostics: DB probe failed: {}", e.getMessage());
        }
        // DIAGNOSTIC: the smoking gun for committed-but-ack-lost. Every durable pfm_operation whose
        // op_id the client never saw acked. If projected>acked is the safe (no-data-loss) direction,
        // this set has exactly (projected-acked) rows, their amounts sum to the net gap, and their ts
        // falls in the node3-rejoin window — proving the read model is right and the *client* under-
        // counted, not the framework. If instead this set is empty while projected>acked, the drift is
        // something else (e.g. an over-projection) and the commit-ambiguity theory is wrong.
        dumpDurableButUnacked();
        // retry-all report per node: failed>0 ⇒ a retry is throwing; total>0 yet lag persists ⇒
        // retried-but-not-applied; total=0 while durable>projected ⇒ the event never reached the
        // DLQ at all (silent dispatch skip).
        for (int node = 0; node < NODE_COUNT; node++) {
            try {
                LOGGER.error("diagnostics: /dlq/retry-all node{} → {}", node + 1,
                        post(httpBase(node) + "/dlq/retry-all", null));
            } catch (RuntimeException e) {
                LOGGER.warn("diagnostics: retry-all on node{} unreachable: {}", node + 1, e.getMessage());
            }
        }
    }

    /**
     * DIAGNOSTIC: list every durable {@code pfm_operation} whose op_id was never acked by the client —
     * the committed-but-ack-lost set. Best-effort; never throws (guards the original assertion).
     */
    private void dumpDurableButUnacked() {
        LoadGenerator load = activeLoad;
        if (load == null) {
            LOGGER.error("diagnostics: durable-but-unacked: no active load generator captured");
            return;
        }
        Set<String> acked = load.ackedOpIds();
        LOGGER.error("diagnostics: durable-but-unacked ops (committed yet client never saw a 2xx). "
                + "acked-opIds tracked={}, failed ops={}", acked.size(), load.failedOps());
        long count = 0;
        long netGap = 0;
        try (Connection c = db();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT op_id, op_type, amount, tag, ts FROM pfm_operation")) {
            while (rs.next()) {
                String opId = uuidFromBytes(rs.getBytes("op_id"));
                if (acked.contains(opId)) continue;
                String type = rs.getString("op_type");
                long amount = rs.getLong("amount");
                count++;
                netGap += "IN".equals(type) ? amount : -amount;
                LOGGER.error("    UNACKED op_id={}, type={}, amount={}, tag={}, ts={}",
                        opId, type, amount, rs.getString("tag"), rs.getLong("ts"));
            }
            LOGGER.error("    durable-but-unacked total: {} ops, net {} "
                    + "(should match projected-acked and the net 'off by' above)", count, netGap);
        } catch (SQLException e) {
            LOGGER.warn("diagnostics: durable-but-unacked probe failed: {}", e.getMessage());
        }
    }

    /** UUID from the BINARY(16) big-endian layout the read store writes (mirrors {@code Uuids.fromBytes}). */
    private static String uuidFromBytes(byte[] bytes) {
        java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(bytes);
        return new java.util.UUID(bb.getLong(), bb.getLong()).toString();
    }

    /** Run one diagnostic query and log every row with its column labels. Never throws. */
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

    private Set<Integer> ownedBy(String node) {
        return segmentOwners().entrySet().stream()
                .filter(e -> node.equals(e.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    /**
     * Let the flood run for {@code ms}, logging an acked-ops + throughput heartbeat every couple of
     * seconds so a tail of the log shows the load actually flowing (and the rate dropping while node3
     * is out of rotation, then recovering once it rejoins).
     */
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
        // Snapshot the framework hot-path metrics for this load window (rate over the phase length)
        // and append them to target/kf-metrics-<TestClass>.csv. Best-effort; no-op without Prometheus.
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
