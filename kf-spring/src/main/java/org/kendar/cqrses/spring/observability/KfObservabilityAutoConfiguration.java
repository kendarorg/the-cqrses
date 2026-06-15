package org.kendar.cqrses.spring.observability;

import io.micrometer.core.instrument.MeterRegistry;
import org.kendar.cqrses.observability.NullObservability;
import org.kendar.cqrses.observability.ObservabilityInterface;
import org.kendar.cqrses.spring.KfProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configures the Micrometer-backed {@link ObservabilityInterface}.
 *
 * <p>Gated by {@link ConditionalOnClass}({@code MeterRegistry}) so an app that
 * does not pull Micrometer (it is an <i>optional</i> dependency of {@code kf-spring},
 * hence non-transitive) never loads this class — no {@code NoClassDefFoundError}.
 * The {@code MeterRegistry} itself is resolved lazily through an
 * {@link ObjectProvider}: when none is present (e.g. no actuator on the classpath)
 * the bean falls back to {@link NullObservability}, so {@code KfBootstrap} can wire
 * it unconditionally. Disable entirely with {@code kf.observability.enabled=false}.
 */
@AutoConfiguration
@ConditionalOnClass(MeterRegistry.class)
@ConditionalOnProperty(name = "kf.observability.enabled", matchIfMissing = true)
@EnableConfigurationProperties(KfProperties.class)
public class KfObservabilityAutoConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(KfObservabilityAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean(ObservabilityInterface.class)
    public ObservabilityInterface kfMicrometerTimers(ObjectProvider<MeterRegistry> registry,
                                                     KfProperties properties) {
        MeterRegistry reg = registry.getIfAvailable();
        if (reg == null) {
            LOGGER.info("kf observability: no MeterRegistry bean; metrics disabled (no-op)");
            return new NullObservability();
        }
        String nodeId = properties.getCluster().getNodeId();
        boolean extended = properties.getObservability().isExtendedMetrics();
        LOGGER.info("kf observability: Micrometer timers enabled (node={}, extendedMetrics={})",
                (nodeId == null || nodeId.isBlank()) ? "single" : nodeId, extended);
        return new MicrometerTimers(reg, nodeId, extended);
    }
}
