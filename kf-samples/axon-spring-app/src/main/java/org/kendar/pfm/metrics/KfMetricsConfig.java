package org.kendar.pfm.metrics;

import net.ttddyy.dsproxy.support.ProxyDataSource;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;
import org.axonframework.config.ConfigurerModule;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * Wires Axon hot paths into {@link KfMeters} so the app emits the SAME {@code kf.*} meters the
 * kf cluster-IT's Prometheus report queries. Hooks here (the command-handler timer lives in
 * {@link KfCommandTimer}, separated to avoid a bean cycle with Axon's configurer):
 *
 * <ol>
 *   <li><b>Event dispatch timer</b> — a default handler interceptor on every streaming processor
 *       (the {@link ConfigurerModule}) → {@code kf.event.dispatch} (group=processor name; segment is
 *       best-effort 0 — Axon doesn't surface it to the interceptor, and the report sums across
 *       segments anyway).</li>
 *   <li><b>Storage-engine timer</b> — a {@code BeanPostProcessor} wraps the auto-configured
 *       {@link EventStorageEngine} in {@link TimingEventStorageEngine} → {@code kf.events.append},
 *       {@code kf.aggregate.rehydrate}, {@code kf.segment.tail.read}.</li>
 *   <li><b>SQL timer</b> — a {@code BeanPostProcessor} wraps the {@link DataSource} with
 *       datasource-proxy; each statement is bucketed by {@link SqlCategory} → {@code kf.sql.execute}.
 *       (datasource-proxy reports elapsed time in milliseconds, so sub-ms statements round to 0 — a
 *       resolution caveat vs kf's nanosecond {@code Db} timing, noted for the comparison.)</li>
 * </ol>
 *
 * The {@code axon-micrometer} auto-configuration still publishes its own {@code commandBus.*} /
 * {@code eventProcessor.*} meters alongside these; the report keys off the {@code kf.*} names.
 */
@Configuration
public class KfMetricsConfig {

    /** Event-dispatch timing: a default handler interceptor on every (streaming) event processor. */
    @Bean
    public ConfigurerModule kfEventDispatchTimer(ObjectProvider<KfMeters> meters) {
        return configurer -> configurer.eventProcessing().registerDefaultHandlerInterceptor(
                (config, processorName) -> (unitOfWork, chain) -> {
                    long t0 = System.nanoTime();
                    boolean ok = true;
                    try {
                        return chain.proceed();
                    } catch (Exception e) {
                        ok = false;
                        throw e;
                    } finally {
                        KfMeters m = meters.getIfAvailable();
                        if (m != null) {
                            m.onEventDispatched(processorName, 0,
                                    unitOfWork.getMessage().getPayloadType().getSimpleName(),
                                    System.nanoTime() - t0, ok);
                        }
                    }
                });
    }

    /** Wrap the auto-configured event storage engine to time append / rehydrate / tail-read. */
    @Bean
    public static BeanPostProcessor kfStorageEngineTimer(ObjectProvider<KfMeters> meters) {
        return new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(Object bean, String beanName) {
                if (bean instanceof EventStorageEngine engine && !(bean instanceof TimingEventStorageEngine)) {
                    return new TimingEventStorageEngine(engine, meters::getObject);
                }
                return bean;
            }
        };
    }

    /** Wrap the datasource so every statement is timed and bucketed into kf.sql.execute{category}. */
    @Bean
    public static BeanPostProcessor kfSqlTimer(ObjectProvider<KfMeters> meters) {
        return new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(Object bean, String beanName) {
                if (bean instanceof DataSource ds && !(bean instanceof ProxyDataSource)) {
                    return ProxyDataSourceBuilder.create(ds)
                            .name("kf-sql")
                            .listener(new net.ttddyy.dsproxy.listener.QueryExecutionListener() {
                                @Override
                                public void beforeQuery(net.ttddyy.dsproxy.ExecutionInfo execInfo,
                                                        java.util.List<net.ttddyy.dsproxy.QueryInfo> queries) {
                                }

                                @Override
                                public void afterQuery(net.ttddyy.dsproxy.ExecutionInfo execInfo,
                                                       java.util.List<net.ttddyy.dsproxy.QueryInfo> queries) {
                                    KfMeters m = meters.getIfAvailable();
                                    if (m == null) {
                                        return;
                                    }
                                    long nanos = execInfo.getElapsedTime() * 1_000_000L;
                                    boolean ok = execInfo.isSuccess();
                                    for (net.ttddyy.dsproxy.QueryInfo q : queries) {
                                        m.onSqlExecuted(SqlCategory.of(q.getQuery()), nanos, ok);
                                    }
                                }
                            })
                            .build();
                }
                return bean;
            }
        };
    }
}
