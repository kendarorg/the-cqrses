package org.kendar.pfm.cluster;

import org.junit.jupiter.api.Test;
import org.kendar.cqrses.cluster.ClusterNode;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.pfm.PfmApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Requirement #4: the cluster-control endpoints exist but <b>do nothing</b> when the app is not in
 * cluster mode. Boots the real PFM app with {@code kf.cluster.enabled=false} (the demo default) on
 * an in-memory H2 and asserts {@code /cluster/status|stop|start} all return {@code enabled=false},
 * never touch a {@link ClusterNode} bean (there is none), and leave {@code running=false}.
 *
 * <p>No Docker — a plain Boot launch via {@link SpringApplicationBuilder} (same path as
 * {@code FinanceFlowTest}, so {@code kf.segments} reaches the starter's {@code EnvironmentPostProcessor}).
 */
class ClusterDisabledNoOpIT {

    private static final AtomicInteger DB_SEQ = new AtomicInteger();

    @Test
    void clusterEndpointsAreInertWhenDisabled() {
        GlobalRegistry.clear();

        String dbUrl = "jdbc:h2:mem:pfm_noop_" + DB_SEQ.incrementAndGet()
                + ";MODE=MySQL;DB_CLOSE_DELAY=-1";

        SpringApplicationBuilder app = new SpringApplicationBuilder(PfmApplication.class)
                .web(WebApplicationType.SERVLET);

        try (ConfigurableApplicationContext ctx = app.run(
                "--server.port=0",
                "--kf.segments=4",
                "--kf.liveness.port=8092",
                "--kf.cluster.enabled=false",
                "--pfm.datasource.url=" + dbUrl)) {

            // The whole point: no cluster beans exist when disabled.
            assertThat(ctx.getBeanNamesForType(ClusterNode.class)).isEmpty();

            int port = Integer.parseInt(ctx.getEnvironment().getProperty("local.server.port"));
            String base = "http://localhost:" + port;
            TestRestTemplate rest = new TestRestTemplate();

            // status → disabled, not running.
            Map<String, Object> status = get(rest, base + "/cluster/status");
            assertThat(status.get("enabled")).isEqualTo(Boolean.FALSE);
            assertThat(status.get("running")).isEqualTo(Boolean.FALSE);

            // stop → still disabled, still not running; changed nothing.
            Map<String, Object> stopped = post(rest, base + "/cluster/stop");
            assertThat(stopped.get("enabled")).isEqualTo(Boolean.FALSE);
            assertThat(stopped.get("running")).isEqualTo(Boolean.FALSE);

            // start → disabled stays a no-op; never flips running true.
            Map<String, Object> started = post(rest, base + "/cluster/start");
            assertThat(started.get("enabled")).isEqualTo(Boolean.FALSE);
            assertThat(started.get("running")).isEqualTo(Boolean.FALSE);

            // Still no cluster bean materialised by any of those calls.
            assertThat(ctx.getBeanNamesForType(ClusterNode.class)).isEmpty();
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> get(TestRestTemplate rest, String url) {
        ResponseEntity<Map> resp = rest.getForEntity(url, Map.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        return resp.getBody();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> post(TestRestTemplate rest, String url) {
        ResponseEntity<Map> resp = rest.postForEntity(url, null, Map.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        return resp.getBody();
    }
}
