package org.kendar.pfm.cluster;

import org.junit.jupiter.api.Test;
import org.kendar.pfm.AxonPfmApplication;
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
 * Axon counterpart of the kf sample's {@code ClusterDisabledNoOpIT}: the cluster-control endpoints
 * exist but <b>do nothing</b> when the app is not in cluster mode. Boots the real Axon PFM app with
 * {@code pfm.cluster.mode=false} (the demo default) on in-memory H2 and asserts
 * {@code /cluster/status|stop|start} all report {@code enabled=false} and leave {@code running=false}.
 *
 * <p>Unlike kf — where the {@code ClusterNode} bean is absent when disabled — server-less Axon always
 * runs its streaming processors locally (single-node still needs its projections). So the contract is
 * asserted at the HTTP/behaviour level: the control surface reports disabled and the operations are
 * inert, never flipping {@code running} true. No Docker.
 */
class ClusterDisabledNoOpIT {

    private static final AtomicInteger DB_SEQ = new AtomicInteger();

    @Test
    void clusterEndpointsAreInertWhenDisabled() {
        String dbUrl = "jdbc:h2:mem:axonpfm_noop_" + DB_SEQ.incrementAndGet()
                + ";MODE=MySQL;DB_CLOSE_DELAY=-1";

        SpringApplicationBuilder app = new SpringApplicationBuilder(AxonPfmApplication.class)
                .web(WebApplicationType.SERVLET);

        try (ConfigurableApplicationContext ctx = app.run(
                "--server.port=0",
                "--pfm.segments=4",
                "--pfm.cluster.mode=false",
                "--spring.datasource.url=" + dbUrl)) {

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
