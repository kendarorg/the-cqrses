package org.kendar.cqrses.spring;

import org.kendar.cqrses.bus.Bus;
import org.kendar.cqrses.bus.CommandBus;
import org.kendar.cqrses.bus.EventBus;
import org.kendar.cqrses.bus.JdbcCommandBus;
import org.kendar.cqrses.bus.JdbcEventBus;
import org.kendar.cqrses.cluster.ClusterNode;
import org.kendar.cqrses.cluster.ClusterNodeBuilder;
import org.kendar.cqrses.cluster.ItemProcessor;
import org.kendar.cqrses.cluster.SegmentItemProcessor;
import org.kendar.cqrses.db.Db;
import org.kendar.cqrses.db.DefaultDb;
import org.kendar.cqrses.db.SchemaInitializer;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.dlq.DlqStore;
import org.kendar.cqrses.observability.InMemoryTraceSink;
import org.kendar.cqrses.observability.ObservabilityInterface;
import org.kendar.cqrses.observability.TraceSink;
import org.kendar.cqrses.pg.LocalSegmentOwner;
import org.kendar.cqrses.pg.SegmentCalculator;
import org.kendar.cqrses.pg.SegmentProcessor;
import org.kendar.cqrses.repositories.CheckpointStore;
import org.kendar.cqrses.repositories.EventStore;
import org.kendar.cqrses.repositories.JdbcCheckpointStore;
import org.kendar.cqrses.repositories.JdbcDlqStore;
import org.kendar.cqrses.repositories.JdbcEventStore;
import org.kendar.cqrses.repositories.JdbcSagaStore;
import org.kendar.cqrses.repositories.SagaStore;
import org.kendar.cqrses.scheduler.JdbcScheduler;
import org.kendar.cqrses.scheduler.Scheduler;
import org.kendar.cqrses.serialization.JacksonMessageSerializer;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.serialization.Upcaster;
import org.kendar.cqrses.serialization.UpcastersManager;
import org.kendar.cqrses.spring.observability.KfPumpLagSampler;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;
import java.util.List;

/**
 * Spring Boot auto-configuration for the kf-core-db single-node stack plus an optional kf-cluster
 * node. All infra beans are {@code @ConditionalOnMissingBean}, so an app can override any single
 * one. {@link KfBootstrap} owns the ordered registration / lifecycle.
 *
 * <p>The app must provide a {@code @Bean("kf-datasource") DataSource}; the context fails fast if it
 * is absent (and no custom {@link Db} bean is supplied). This keeps the framework DB segregated from
 * the app's own (possibly multiple) data sources.
 */
@AutoConfiguration
@EnableConfigurationProperties(KfProperties.class)
public class KfAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public Db kfDb(@Qualifier("kf-datasource") DataSource dataSource) {
        Db db = new DefaultDb(dataSource);
        // The JDBC stores assume their schema exists; create it before any store touches the DB.
        new SchemaInitializer(db).initialize();
        return db;
    }

    @Bean
    @ConditionalOnMissingBean
    public MessageSerializer<?, ?> kfMessageSerializer() {
        MessageSerializer<?, ?> serializer = new JacksonMessageSerializer();
        // Register eagerly: JdbcEventStore's BaseEventStore ctor reads GlobalRegistry.get(...) at
        // construction (during context refresh, before KfBootstrap.start), so the serializer and
        // UpcastersManager must already be resolvable then. KfBootstrap re-registers them (idempotent).
        GlobalRegistry.register(MessageSerializer.class, serializer);
        return serializer;
    }

    @Bean
    @ConditionalOnMissingBean
    public UpcastersManager kfUpcastersManager(MessageSerializer<?, ?> serializer,
                                               ObjectProvider<Upcaster> upcasters) {
        UpcastersManager manager = new UpcastersManager(serializer, upcasters.orderedStream().toList());
        GlobalRegistry.register(UpcastersManager.class, manager);
        return manager;
    }

    @Bean
    @ConditionalOnMissingBean
    public EventStore kfEventStore(Db db, UpcastersManager upcastersManager) {
        // upcastersManager param forces ordering: it (and the serializer) are in GlobalRegistry
        // before JdbcEventStore's ctor reads them.
        return new JdbcEventStore(db);
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaStore kfSagaStore(Db db) {
        return new JdbcSagaStore(db);
    }

    @Bean
    @ConditionalOnMissingBean
    public DlqStore kfDlqStore(Db db) {
        return new JdbcDlqStore(db);
    }

    @Bean
    @ConditionalOnMissingBean
    public Scheduler kfScheduler(Db db) {
        return new JdbcScheduler(db);
    }

    @Bean
    @ConditionalOnMissingBean
    public CommandBus kfCommandBus(MessageSerializer<?, ?> serializer,
                                   EventStore eventStore,
                                   DlqStore dlqStore) {
        return new JdbcCommandBus(serializer, eventStore, dlqStore);
    }

    @Bean
    @ConditionalOnMissingBean
    public EventBus kfEventBus(MessageSerializer<?, ?> serializer,
                               SagaStore sagaStore,
                               DlqStore dlqStore,
                               KfProperties properties,
                               ObjectProvider<EventBusCustomizer> customizers) {
        EventBus eventBus = new JdbcEventBus(serializer, sagaStore, dlqStore);
        for (String group : properties.getProcessingGroups()) {
            eventBus.setProcessingGroupPolicy(Bus.defaultProcessingGroupPolicyConfig(group));
        }
        customizers.orderedStream().forEach(c -> c.customize(eventBus));
        return eventBus;
    }

    @Bean
    @ConditionalOnMissingBean
    public CheckpointStore kfCheckpointStore(Db db) {
        return new JdbcCheckpointStore(db);
    }

    /**
     * The event-side pull engine. Built over the event bus's
     * {@code ProcessingGroupsManager} (which {@link KfBootstrap} flips into pull
     * mode before starting), the durable {@link EventStore} and the
     * {@link CheckpointStore}. The JDBC event side is always pull mode — single-node
     * and cluster alike — so this bean is created unconditionally.
     */
    @Bean
    @ConditionalOnMissingBean
    public SegmentProcessor kfSegmentProcessor(EventBus eventBus,
                                               EventStore eventStore,
                                               CheckpointStore checkpointStore,
                                               KfProperties properties) {
        return new SegmentProcessor(eventBus.getHandler(), eventStore, checkpointStore,
                SegmentProcessor.DEFAULT_BATCH, properties.getCluster().getDispatchConcurrency());
    }

    /**
     * Single-node (cluster-disabled) driver of the {@link SegmentProcessor}: claims
     * all {@code 0..segments-1} segments on parked daemon threads so the event side
     * pulls on the local node. Under the cluster the {@link SegmentItemProcessor}
     * drives segment ownership instead, so this bean is omitted then.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "kf.cluster", name = "enabled", havingValue = "false", matchIfMissing = true)
    public LocalSegmentOwner kfLocalSegmentOwner(SegmentProcessor segmentProcessor) {
        return new LocalSegmentOwner(segmentProcessor, SegmentCalculator.getSegments());
    }

    /**
     * The real cluster {@link ItemProcessor}: the thin adapter bridging the
     * cluster partition lifecycle to the {@link SegmentProcessor} pull engine.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "kf.cluster", name = "enabled", havingValue = "true")
    public ItemProcessor kfSegmentItemProcessor(SegmentProcessor segmentProcessor, ClusterNode clusterNode) {
        return new SegmentItemProcessor(segmentProcessor, clusterNode);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "kf.cluster", name = "enabled", havingValue = "true")
    public ClusterNode kfClusterNode(Db db, KfProperties properties) {
        ClusterNodeBuilder builder = ClusterNode.builder().db(db);
        String nodeId = properties.getCluster().getNodeId();
        if (nodeId != null && !nodeId.isBlank()) {
            builder.nodeId(nodeId);
        }
        String host = properties.getCluster().getHost();
        if (host != null && !host.isBlank()) {
            builder.host(host);
        }
        if (properties.getCluster().getForwarding().isEnabled()) {
            builder.forwardPort(properties.getCluster().getForwarding().getPort());
        }
        return builder.build();
    }

    @Bean
    @ConditionalOnMissingBean
    public KfBootstrap kfBootstrap(ApplicationContext context,
                                   KfProperties properties,
                                   MessageSerializer<?, ?> serializer,
                                   UpcastersManager upcastersManager,
                                   Db db,
                                   EventStore eventStore,
                                   SagaStore sagaStore,
                                   DlqStore dlqStore,
                                   CommandBus commandBus,
                                   EventBus eventBus,
                                   Scheduler scheduler,
                                   ObjectProvider<ClusterNode> clusterNode,
                                   ObjectProvider<ItemProcessor> itemProcessor,
                                   SegmentProcessor segmentProcessor,
                                   ObjectProvider<LocalSegmentOwner> localSegmentOwner,
                                   ObjectProvider<ObservabilityInterface> observability,
                                   ObjectProvider<TraceSink> traceSink) {
        return new KfBootstrap(context, properties, serializer, upcastersManager, db, eventStore,
                sagaStore, dlqStore, commandBus, eventBus, scheduler, clusterNode, itemProcessor,
                segmentProcessor, localSegmentOwner, observability, traceSink);
    }

    /**
     * Bounded in-memory ring of sampled per-command perf traces — only when
     * {@code kf.observability.trace.enabled=true}. Deliberately NOT a JDBC sink:
     * perf data written to the measured database would perturb the measurement.
     * Harvested over HTTP by {@code KfPerfTraceController}.
     */
    @Bean
    @ConditionalOnProperty(name = "kf.observability.trace.enabled")
    @ConditionalOnMissingBean(TraceSink.class)
    public InMemoryTraceSink kfTraceSink(KfProperties properties) {
        return new InMemoryTraceSink(properties.getObservability().getTrace().getBufferCapacity());
    }

    /**
     * Pull-pump backlog sampler, part of the extended bottleneck metrics. Two
     * small SELECTs per interval; publishes {@code kf.pump.lag} gauges.
     */
    @Bean
    @ConditionalOnProperty(name = "kf.observability.extended-metrics")
    @ConditionalOnMissingBean
    public KfPumpLagSampler kfPumpLagSampler(Db db, KfProperties properties) {
        return new KfPumpLagSampler(db, properties.getObservability().getLag().getSampleIntervalMs());
    }
}
