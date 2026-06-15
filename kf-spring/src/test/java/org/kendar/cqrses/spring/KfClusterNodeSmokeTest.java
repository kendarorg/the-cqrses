package org.kendar.cqrses.spring;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.db.Db;
import org.kendar.cqrses.di.GlobalRegistry;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.sql.DataSource;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test #4: single-node {@code ClusterNode} start/stop smoke test (H2-backed) through the kf-spring
 * cluster path. With {@code kf.cluster.enabled=true}, {@link KfBootstrap} starts the node with
 * {@code N == SegmentCalculator.getSegments()} (set from {@code kf.segments} by the post-processor)
 * and the {@code kf.liveness.port}. Closing the context drives the clean {@code stop()} reversal.
 */
class KfClusterNodeSmokeTest {

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @Test
    void clusterNodeStartsAndStopsThroughTheStarter() throws Exception {
        GlobalRegistry.clear();
        int port = freePort();

        SpringApplicationBuilder app = new SpringApplicationBuilder(ClusterTestApp.class)
                .web(WebApplicationType.NONE)
                .properties(
                        "kf.segments=4",
                        "kf.liveness.port=" + port,
                        "kf.cluster.enabled=true",
                        "kf.cluster.node-id=N1",
                        "kf.cluster.host=127.0.0.1");

        try (ConfigurableApplicationContext ctx = app.run()) {
            Db db = ctx.getBean(Db.class);

            // Cluster mode swaps the no-op processor for the real pull bridge, and the
            // event bus is flipped into pull mode by KfBootstrap before it starts.
            assertThat(ctx.getBean(org.kendar.cqrses.cluster.ItemProcessor.class))
                    .isInstanceOf(org.kendar.cqrses.cluster.SegmentItemProcessor.class);
            assertThat(ctx.getBeansOfType(org.kendar.cqrses.pg.SegmentProcessor.class)).hasSize(1);
            assertThat(ctx.getBean(org.kendar.cqrses.bus.EventBus.class).getHandler().isPullMode())
                    .as("event bus must be in pull mode under cluster").isTrue();

            // The bootstrap registered the node row and seeded N == getSegments() == 4.
            assertThat((long) db.queryForObject(
                    "SELECT COUNT(*) FROM cluster_nodes WHERE node_id = 'N1'", Long.class)).isEqualTo(1L);
            assertThat((int) db.queryForObject(
                    "SELECT item_count FROM cluster_config WHERE id = 1", Integer.class)).isEqualTo(4);

            // Liveness endpoint answers.
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
            HttpResponse<String> alive = client.send(HttpRequest.newBuilder()
                            .uri(URI.create("http://127.0.0.1:" + port + "/alive")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(alive.statusCode()).isEqualTo(200);
            assertThat(alive.body()).isEqualTo("alive");
        }
        // try-with-resources close → KfBootstrap.stop() → ClusterNode.stop(): a clean shutdown is the
        // smoke test's second half (no exception escapes).
    }

    @Configuration
    @Import(KfAutoConfiguration.class)
    static class ClusterTestApp {

        private static final AtomicInteger DB_SEQ = new AtomicInteger();

        @Bean("kf-datasource")
        DataSource kfDataSource() {
            JdbcDataSource ds = new JdbcDataSource();
            ds.setURL("jdbc:h2:mem:kfspring_cluster_" + DB_SEQ.incrementAndGet()
                    + ";MODE=MySQL;DB_CLOSE_DELAY=-1");
            return ds;
        }
    }
}
