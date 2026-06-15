package org.kendar.pfm;

import org.junit.jupiter.api.Test;
import org.kendar.cqrses.di.GlobalRegistry;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * End-to-end HTTP slice over the full auto-wired stack (kf-spring starter → kf-core-db single-node,
 * cluster disabled), on an in-memory H2 ({@code MODE=MySQL}) database. Drives the real REST surface
 * with a {@link TestRestTemplate}: login creates a user (idempotent on re-login), operations flow as
 * commands → events → the durable projections, and the summary endpoints reflect them. A re-applied
 * operation must not double-count (idempotent read model).
 *
 * <p>Launched via {@link SpringApplicationBuilder} (not {@code @SpringBootTest}) so {@code kf.segments}
 * / {@code kf.liveness.port} are visible to the starter's {@code EnvironmentPostProcessor} during
 * {@code prepareEnvironment} — exercising the real startup path including the segment hook.
 */
class FinanceFlowTest {

    private static final AtomicInteger DB_SEQ = new AtomicInteger();

    @Test
    void loginRecordOperationsAndReadBackTotals() {
        GlobalRegistry.clear();

        String dbUrl = "jdbc:h2:mem:pfm_test_" + DB_SEQ.incrementAndGet()
                + ";MODE=MySQL;DB_CLOSE_DELAY=-1";

        SpringApplicationBuilder app = new SpringApplicationBuilder(PfmApplication.class)
                .web(WebApplicationType.SERVLET);

        // Pass overrides as command-line args (NOT .properties(), which is the lowest-precedence
        // defaultProperties source and would lose to application.yml's file-based datasource URL).
        // Command-line args outrank application.yml and are still visible to the starter's
        // EnvironmentPostProcessor during prepareEnvironment.
        try (ConfigurableApplicationContext ctx = app.run(
                "--server.port=0",
                "--kf.segments=4",
                "--kf.liveness.port=8091",
                "--pfm.datasource.url=" + dbUrl)) {
            int port = Integer.parseInt(ctx.getEnvironment().getProperty("local.server.port"));
            String base = "http://localhost:" + port;
            TestRestTemplate rest = new TestRestTemplate();

            // First login registers the user.
            Map<String, Object> login1 = post(rest, base + "/api/login", Map.of("username", "alice"));
            assertThat(login1.get("created")).isEqualTo(Boolean.TRUE);
            String userId = (String) login1.get("userId");

            // Second login reuses the same user (idempotent), once the projection has caught up.
            awaitUntil(() -> Boolean.FALSE.equals(
                    post(rest, base + "/api/login", Map.of("username", "alice")).get("created")));
            Map<String, Object> login2 = post(rest, base + "/api/login", Map.of("username", "alice"));
            assertThat(login2.get("created")).isEqualTo(Boolean.FALSE);
            assertThat(login2.get("userId")).isEqualTo(userId);

            // Record three operations.
            String op1 = recordOp(rest, base, "alice", "IN", 1000, "salary");
            recordOp(rest, base, "alice", "OUT", 300, "rent");
            recordOp(rest, base, "alice", "OUT", 200, "groceries");

            // Wait for the read model to reflect them.
            awaitUntil(() -> summary(rest, base, "alice").get("net").longValue() == 500L);

            Map<String, Long> summary = summary(rest, base, "alice");
            assertThat(summary.get("in")).isEqualTo(1000L);
            assertThat(summary.get("out")).isEqualTo(500L);
            assertThat(summary.get("net")).isEqualTo(500L);

            // Per-tag breakdown.
            List<Map<String, Object>> byTag = byTag(rest, base, "alice");
            assertThat(byTag).extracting(m -> (String) m.get("tag"))
                    .containsExactlyInAnyOrder("salary", "rent", "groceries");

            // Operations list has all three.
            List<Map<String, Object>> ops = operations(rest, base, "alice");
            assertThat(ops).hasSize(3);

            // Idempotency: re-applying op1's event (same op_id) must not change totals. We can't
            // resend through HTTP (server mints a new opId), so we hit the read store directly with
            // the same op_id and assert no drift.
            var store = ctx.getBean(org.kendar.pfm.read.OperationReadStore.class);
            var aliceId = org.kendar.pfm.read.Uuids.userId("alice");
            var firstOp = ops.stream().filter(o -> "salary".equals(o.get("tag"))).findFirst().orElseThrow();
            var dupOpId = java.util.UUID.fromString((String) firstOp.get("opId"));
            store.record(dupOpId, aliceId, org.kendar.pfm.domain.OpType.IN, 1000, "salary", 1L);
            assertThat(summary(rest, base, "alice").get("net")).isEqualTo(500L);
        }
    }

    private static String recordOp(TestRestTemplate rest, String base, String user, String type,
                                   long amount, String tag) {
        Map<String, Object> r = post(rest, base + "/api/operations",
                Map.of("username", user, "type", type, "amount", amount, "tag", tag));
        return (String) r.get("opId");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> post(TestRestTemplate rest, String url, Map<String, Object> body) {
        ResponseEntity<Map> resp = rest.postForEntity(url, body, Map.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        return resp.getBody();
    }

    private static Map<String, Long> summary(TestRestTemplate rest, String base, String user) {
        return rest.exchange(base + "/api/summary?username=" + user, org.springframework.http.HttpMethod.GET,
                null, new ParameterizedTypeReference<Map<String, Long>>() {
                }).getBody();
    }

    private static List<Map<String, Object>> byTag(TestRestTemplate rest, String base, String user) {
        return rest.exchange(base + "/api/summary/by-tag?username=" + user,
                org.springframework.http.HttpMethod.GET, null,
                new ParameterizedTypeReference<List<Map<String, Object>>>() {
                }).getBody();
    }

    private static List<Map<String, Object>> operations(TestRestTemplate rest, String base, String user) {
        return rest.exchange(base + "/api/operations?username=" + user,
                org.springframework.http.HttpMethod.GET, null,
                new ParameterizedTypeReference<List<Map<String, Object>>>() {
                }).getBody();
    }

    private static void awaitUntil(BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return;
            try {
                Thread.sleep(20);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                fail("interrupted");
            }
        }
        fail("Condition did not become true within 5s");
    }
}
