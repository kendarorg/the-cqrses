package org.kendar.cqrses.spring;

import org.kendar.cqrses.annotations.CommandHandler;
import org.kendar.cqrses.annotations.EventHandler;
import org.kendar.cqrses.bus.CommandBus;
import org.kendar.cqrses.bus.Context;
import org.kendar.cqrses.bus.EventBus;
import org.kendar.cqrses.cluster.ClusterNode;
import org.kendar.cqrses.cluster.ItemProcessor;
import org.kendar.cqrses.cluster.spi.CommandForwarding;
import org.kendar.cqrses.db.Db;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.dlq.DlqStore;
import org.kendar.cqrses.observability.Observability;
import org.kendar.cqrses.observability.ObservabilityInterface;
import org.kendar.cqrses.observability.TraceRecorder;
import org.kendar.cqrses.observability.TraceSink;
import org.kendar.cqrses.pg.LocalSegmentOwner;
import org.kendar.cqrses.pg.SegmentCalculator;
import org.kendar.cqrses.pg.SegmentProcessor;
import org.kendar.cqrses.repositories.EventStore;
import org.kendar.cqrses.repositories.SagaStore;
import org.kendar.cqrses.scheduler.Scheduler;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.serialization.UpcastersManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.context.SmartLifecycle;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.kendar.cqrses.utils.ReflectionUtils.getMethodsAnnotatedWith;

/**
 * Owns the ordered framework setup sequence. The infra are plain {@code @Bean}s; this
 * {@link SmartLifecycle} runs at a late phase ({@link #getPhase()}) once every singleton is
 * constructed, then registers everything into {@code GlobalRegistry} in the order the framework's
 * setup/runtime split demands (everything frozen before the first {@code send}/{@code publish}).
 *
 * <p>Hard ordering: {@code autoSubscribe} casts {@code registry.get(CommandBus.class)} /
 * {@code EventBus.class} with <b>no null check</b> — so the buses must be registered into
 * {@code GlobalRegistry} before any handler.
 */
public class KfBootstrap implements SmartLifecycle {

    private static final Logger LOGGER = LoggerFactory.getLogger(KfBootstrap.class);

    private final ApplicationContext context;
    private final KfProperties properties;
    private final MessageSerializer<?, ?> serializer;
    private final UpcastersManager upcastersManager;
    private final Db db;
    private final EventStore eventStore;
    private final SagaStore sagaStore;
    private final DlqStore dlqStore;
    private final CommandBus commandBus;
    private final EventBus eventBus;
    private final Scheduler scheduler;
    private final ObjectProvider<ClusterNode> clusterNode;
    private final ObjectProvider<ItemProcessor> itemProcessor;
    private final SegmentProcessor segmentProcessor;
    private final ObjectProvider<LocalSegmentOwner> localSegmentOwner;
    private final ObjectProvider<ObservabilityInterface> observability;
    private final ObjectProvider<TraceSink> traceSink;

    private volatile boolean running = false;

    public KfBootstrap(ApplicationContext context,
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
        this.context = context;
        this.properties = properties;
        this.serializer = serializer;
        this.upcastersManager = upcastersManager;
        this.db = db;
        this.eventStore = eventStore;
        this.sagaStore = sagaStore;
        this.dlqStore = dlqStore;
        this.commandBus = commandBus;
        this.eventBus = eventBus;
        this.scheduler = scheduler;
        this.clusterNode = clusterNode;
        this.itemProcessor = itemProcessor;
        this.segmentProcessor = segmentProcessor;
        this.localSegmentOwner = localSegmentOwner;
        this.observability = observability;
        this.traceSink = traceSink;
    }

    @Override
    public void start() {
        if (running) return;

        // 2. Register infra into GlobalRegistry — buses FIRST (autoSubscribe casts
        //    registry.get(CommandBus/EventBus) with no null check when a handler registers).
        GlobalRegistry.register(CommandBus.class, commandBus);
        GlobalRegistry.register(EventBus.class, eventBus);
        GlobalRegistry.register(MessageSerializer.class, serializer);
        GlobalRegistry.register(UpcastersManager.class, upcastersManager);
        GlobalRegistry.register(Db.class, db);
        GlobalRegistry.register(EventStore.class, eventStore);
        GlobalRegistry.register(SagaStore.class, sagaStore);
        GlobalRegistry.register(DlqStore.class, dlqStore);
        GlobalRegistry.register(Scheduler.class, scheduler);

        // 2b. Install the observability sink (Micrometer timers, or no-op) into the
        //     kf-core static holder BEFORE any dispatch can run. We use the holder,
        //     not GlobalRegistry, so the per-message hot path stays off the map.
        Observability.set(observability.getIfAvailable());

        // 2c. Install the sampled per-command trace recorder (setup phase only, per
        //     the frozen-topology rule). The sink bean only exists when
        //     kf.observability.trace.enabled=true.
        TraceSink sink = traceSink.getIfAvailable();
        if (sink != null) {
            TraceRecorder.install(sink, properties.getObservability().getTrace().getSampleEvery());
            LOGGER.info("kf perf tracing enabled: sampleEvery={}, bufferCapacity={}",
                    properties.getObservability().getTrace().getSampleEvery(),
                    properties.getObservability().getTrace().getBufferCapacity());
        }

        // 3. Collaborator resolution: handler method-parameter collaborators are pulled from Spring
        //    lazily and cached back into the registry.
        GlobalRegistry.setFallbackResolver(type -> context.getBeanProvider(type).getIfAvailable());

        // 4. Discover handlers/aggregates/sagas/projections and bridge them into GlobalRegistry.
        List<Class<?>> handlerTypes = scanAndRegisterHandlers();

        // 5. Pre-warm collaborators so no bean is created on a dispatch thread.
        preWarmCollaborators(handlerTypes);

        // 5b. Flip the event side into pull mode BEFORE the bus builds its lanes.
        //     The JDBC event side is ALWAYS pull (single-node and cluster alike):
        //     the SegmentProcessor (a bean) owns event-side dispatch; the bus spawns
        //     no lane/saga-resolver threads and its local push is a no-op. Single-node
        //     drives it via LocalSegmentOwner; the cluster via SegmentItemProcessor.
        //     The command side stays push.
        var eventHandler = eventBus.getHandler();
        if (eventHandler != null) {
            eventHandler.setPullMode(true);
            LOGGER.info("event bus set to pull mode");
        } else {
            LOGGER.warn("event bus exposes no handler; pull mode NOT enabled — "
                    + "event-side dispatch will not run through the SegmentProcessor");
        }

        // 6. Start both buses (builds lanes).
        GlobalRegistry.start();

        // 7. Start the scheduler.
        scheduler.start();

        // 8. Start the event-side pull driver. Cluster: join the cluster, which drives
        //    the SegmentItemProcessor over the SegmentProcessor (N == getSegments();
        //    livenessPort from properties). Single-node: LocalSegmentOwner claims all
        //    segments locally.
        if (properties.getCluster().isEnabled()) {
            ClusterNode node = clusterNode.getIfAvailable();
            if (node != null) {
                node.start(SegmentCalculator.getSegments(), properties.getLiveness().getPort(),
                        itemProcessor.getObject());
                // 8b. Command forwarding: install the node's forwarder into the
                //     kf-core holder. Still inside the SmartLifecycle phase, i.e.
                //     before any application send — the frozen-before-first-send
                //     rule holds. Null when kf.cluster.forwarding.enabled=false.
                var commandForwarder = node.commandForwarder();
                if (commandForwarder != null) {
                    CommandForwarding.install(commandForwarder);
                    LOGGER.info("command forwarding enabled (port {})",
                            properties.getCluster().getForwarding().getPort());
                }
            } else {
                LOGGER.warn("kf.cluster.enabled=true but no ClusterNode bean is available; cluster not started");
            }
        } else {
            localSegmentOwner.getObject().start();
            LOGGER.info("single-node pull: LocalSegmentOwner started");
        }

        running = true;
        LOGGER.info("kf-spring bootstrap complete: segments={}, cluster={}",
                SegmentCalculator.getSegments(), properties.getCluster().isEnabled());
    }

    private List<Class<?>> scanAndRegisterHandlers() {
        List<String> basePackages = KfHandlerScanner.resolveBasePackages(
                properties.getScan().getBasePackages(), context);
        List<Class<?>> types = KfHandlerScanner.scan(basePackages, context.getClassLoader());
        for (Class<?> type : types) {
            // A Spring bean of that type exists → register(class, bean) (projections, command
            // handlers/interceptors, event handlers). Else → register(class) (aggregates and
            // sagas — kf instantiates these per-id; they are not Spring beans).
            Object bean = context.getBeanProvider(type).getIfAvailable();
            if (bean != null) {
                GlobalRegistry.register(type, bean);
            } else {
                GlobalRegistry.register(type);
            }
        }
        LOGGER.info("kf-spring scanned {} handler/aggregate/saga/projection type(s) in {}",
                types.size(), basePackages);
        return types;
    }

    /**
     * Reflect over every registered handler/aggregate/saga/projection's
     * {@code @CommandHandler}/{@code @EventHandler} methods; for each param past index 0 (skip the
     * {@link Context} param), eagerly {@code GlobalRegistry.get(type)} so no collaborator bean is
     * created on a dispatch thread. Warn — never fail — on an unresolved type: a param may be
     * satisfied by a path we don't model; a truly-missing one surfaces as a hard error at dispatch.
     *
     * <p>This cache-back is exempt from the runtime-freeze contract: it touches neither the bus
     * handler maps nor the policy maps, only the collaborator cache (via race-safe
     * {@code putIfAbsent}), and never triggers {@code autoSubscribe}.
     */
    private void preWarmCollaborators(List<Class<?>> handlerTypes) {
        Set<Class<?>> warmed = new HashSet<>();
        for (Class<?> type : handlerTypes) {
            for (Method method : getMethodsAnnotatedWith(type, CommandHandler.class)) {
                warmParams(method, warmed);
            }
            for (Method method : getMethodsAnnotatedWith(type, EventHandler.class)) {
                warmParams(method, warmed);
            }
        }
    }

    private void warmParams(Method method, Set<Class<?>> warmed) {
        Class<?>[] params = method.getParameterTypes();
        for (int i = 1; i < params.length; i++) {
            Class<?> paramType = params[i];
            if (paramType == Context.class) continue;
            if (!warmed.add(paramType)) continue;
            Object resolved = GlobalRegistry.get(paramType);
            if (resolved == null) {
                LOGGER.warn("pre-warm: collaborator {} (param of {}#{}) did not resolve from "
                                + "GlobalRegistry or Spring; it must be satisfied at dispatch time",
                        paramType.getName(), method.getDeclaringClass().getSimpleName(), method.getName());
            }
        }
    }

    @Override
    public void stop() {
        if (!running) return;
        running = false;
        // Reverse order, each step isolated so one failure cannot block the others.
        // Forwarding hook FIRST: any send racing this shutdown degrades to plain
        // local dispatch instead of hitting a closing transport. ClusterNode.stop
        // (below) then drains the forwarding clients and server.
        CommandForwarding.reset();
        // The pull pump must stop BEFORE the buses: drain the segment driver first,
        // then terminate the SegmentProcessor projection worker threads.
        if (properties.getCluster().isEnabled()) {
            ClusterNode node = clusterNode.getIfAvailable();
            if (node != null) {
                try {
                    node.stop();
                } catch (RuntimeException e) {
                    LOGGER.warn("error stopping ClusterNode: {}", e.getMessage());
                }
            }
        } else {
            try {
                LocalSegmentOwner owner = localSegmentOwner.getIfAvailable();
                if (owner != null) {
                    owner.stop();
                }
            } catch (RuntimeException e) {
                LOGGER.warn("error stopping LocalSegmentOwner: {}", e.getMessage());
            }
        }
        try {
            segmentProcessor.stopAll();
        } catch (RuntimeException e) {
            LOGGER.warn("error stopping SegmentProcessor: {}", e.getMessage());
        }
        try {
            scheduler.stop();
        } catch (RuntimeException e) {
            LOGGER.warn("error stopping Scheduler: {}", e.getMessage());
        }
        try {
            GlobalRegistry.stop();
        } catch (RuntimeException e) {
            LOGGER.warn("error stopping buses via GlobalRegistry: {}", e.getMessage());
        }
        try {
            GlobalRegistry.clear();
        } catch (RuntimeException e) {
            LOGGER.warn("error clearing GlobalRegistry: {}", e.getMessage());
        }
        // Stop tracing before the observability sink so late stage() calls are no-ops.
        TraceRecorder.reset();
        TraceSink sink = traceSink.getIfAvailable();
        if (sink != null) {
            try {
                sink.close();
            } catch (RuntimeException e) {
                LOGGER.warn("error closing TraceSink: {}", e.getMessage());
            }
        }
        // Restore the no-op sink so a late stray callback can't touch a torn-down registry.
        Observability.set(null);
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        // Late phase: start after web servers / data-source pools are up, stop before them.
        return Integer.MAX_VALUE - 1000;
    }
}
