package org.kendar.cqrses.spring;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.bus.CommandBus;
import org.kendar.cqrses.bus.EventBus;
import org.kendar.cqrses.cluster.ClusterNode;
import org.kendar.cqrses.cluster.ItemProcessor;
import org.kendar.cqrses.db.Db;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.dlq.DlqStore;
import org.kendar.cqrses.pg.LocalSegmentOwner;
import org.kendar.cqrses.pg.SegmentProcessor;
import org.kendar.cqrses.repositories.CheckpointStore;
import org.kendar.cqrses.repositories.EventStore;
import org.kendar.cqrses.repositories.SagaStore;
import org.kendar.cqrses.scheduler.Scheduler;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.serialization.UpcastersManager;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test #2: auto-configuration wiring and override behaviour. Every infra bean is present once an
 * app supplies a {@code @Bean("kf-datasource") DataSource}; every {@code @ConditionalOnMissingBean}
 * is overridable; and {@code kf.cluster.enabled=false} (the default) omits the {@link ClusterNode}.
 *
 * <p>The JDBC event side is always pull mode (single-node and cluster alike): cluster-off the
 * {@link SegmentProcessor} is driven by a {@link LocalSegmentOwner}; cluster-on by the
 * {@code SegmentItemProcessor}. So cluster-off there is NO {@link ItemProcessor} bean — the
 * pull driver is the {@link LocalSegmentOwner} instead.
 *
 * <p>{@link ApplicationContextRunner} fully refreshes the context, so {@link KfBootstrap} (a
 * {@code SmartLifecycle} with {@code isAutoStartup()=true}) actually runs its setup sequence against
 * an in-memory H2 ({@code MODE=MySQL}); each run closes afterwards, which clears the static
 * {@code GlobalRegistry} via {@code KfBootstrap.stop()} (and stops the pull pump threads).
 */
class KfAutoConfigurationWiringTest {

    private static final AtomicInteger DB_SEQ = new AtomicInteger();

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KfAutoConfiguration.class))
            .withUserConfiguration(DataSourceConfig.class)
            // kf.liveness.port is bound via @ConfigurationProperties here (consumed only at cluster start).
            .withPropertyValues("kf.liveness.port=8070");

    @BeforeEach
    @AfterEach
    void cleanRegistry() {
        // Guard against a prior run that failed to close cleanly leaving the static registry dirty.
        GlobalRegistry.clear();
    }

    @Test
    void allInfraBeansPresentAndClusterOmittedByDefault() {
        runner.run(ctx -> {
            assertThat(ctx).hasNotFailed();
            assertThat(ctx).hasSingleBean(Db.class);
            assertThat(ctx).hasSingleBean(MessageSerializer.class);
            assertThat(ctx).hasSingleBean(UpcastersManager.class);
            assertThat(ctx).hasSingleBean(EventStore.class);
            assertThat(ctx).hasSingleBean(SagaStore.class);
            assertThat(ctx).hasSingleBean(DlqStore.class);
            assertThat(ctx).hasSingleBean(Scheduler.class);
            assertThat(ctx).hasSingleBean(CommandBus.class);
            assertThat(ctx).hasSingleBean(EventBus.class);
            assertThat(ctx).hasSingleBean(KfBootstrap.class);
            // Always-pull event side: SegmentProcessor + CheckpointStore exist unconditionally,
            // driven cluster-off by a LocalSegmentOwner.
            assertThat(ctx).hasSingleBean(SegmentProcessor.class);
            assertThat(ctx).hasSingleBean(CheckpointStore.class);
            assertThat(ctx).hasSingleBean(LocalSegmentOwner.class);
            // kf.cluster.enabled defaults to false → no cluster node, and no ItemProcessor
            // (the cluster-only SegmentItemProcessor); the pull driver is the LocalSegmentOwner.
            assertThat(ctx).doesNotHaveBean(ClusterNode.class);
            assertThat(ctx).doesNotHaveBean(ItemProcessor.class);
        });
    }

    @Test
    void conditionalOnMissingBeanOverrideWins() {
        // Prove @ConditionalOnMissingBean still backs off, using a bean that exists cluster-off
        // (CheckpointStore — the old ItemProcessor override no longer applies cluster-off).
        runner.withUserConfiguration(CustomCheckpointStoreConfig.class).run(ctx -> {
            assertThat(ctx).hasNotFailed();
            assertThat(ctx).hasSingleBean(CheckpointStore.class);
            // App-supplied bean replaces the default JdbcCheckpointStore.
            assertThat(ctx.getBean(CheckpointStore.class)).isInstanceOf(CustomCheckpointStore.class);
        });
    }

    @Test
    void clusterNodePresentWhenEnabled() {
        runner.withPropertyValues("kf.cluster.enabled=true", "kf.cluster.node-id=N1")
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(ctx).hasSingleBean(ClusterNode.class);
                    // Cluster-on the ItemProcessor (SegmentItemProcessor) drives the pull engine.
                    assertThat(ctx).hasSingleBean(ItemProcessor.class);
                });
    }

    @Configuration
    static class DataSourceConfig {
        @Bean("kf-datasource")
        DataSource kfDataSource() {
            JdbcDataSource ds = new JdbcDataSource();
            // Unique DB per bean creation so sequential context runs never share state.
            ds.setURL("jdbc:h2:mem:kfspring_wiring_" + DB_SEQ.incrementAndGet()
                    + ";MODE=MySQL;DB_CLOSE_DELAY=-1");
            return ds;
        }
    }

    @Configuration
    static class CustomCheckpointStoreConfig {
        @Bean
        CheckpointStore customCheckpointStore() {
            return new CustomCheckpointStore();
        }
    }

    /** App override proving {@code @ConditionalOnMissingBean} backs off. */
    static final class CustomCheckpointStore implements CheckpointStore {
        @Override
        public long load(String processingGroup, int segment, int sourceSegment) {
            return -1;
        }

        @Override
        public void save(String processingGroup, int segment, int sourceSegment, long lastSeq) {
        }

        @Override
        public void reset(String processingGroup, int segment, int sourceSegment, long lastSeq) {
        }
    }
}
