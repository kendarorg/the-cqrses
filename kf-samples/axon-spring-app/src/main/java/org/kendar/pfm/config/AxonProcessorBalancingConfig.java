package org.kendar.pfm.config;

import org.axonframework.config.ConfigurerModule;
import org.kendar.pfm.cluster.HeartbeatService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Makes the pooled streaming processors balance across the cluster instead of letting the first node
 * to boot keep every segment.
 *
 * <p>By default Axon's {@code maxSegmentProvider} is {@code MaxSegmentProvider.maxShort()} — an
 * effectively unbounded cap — so a node greedily claims all {@code segments} of a processor and, since
 * it keeps renewing those claims, never frees any for a peer. There is no Spring property for the cap
 * (only {@code initial-segment-count} is exposed), so we set it programmatically here, and make it
 * <b>dynamic</b>: {@link HeartbeatService#dynamicCap()} returns {@code ceil(segments / liveNodes)},
 * which Axon's coordinator re-reads each cycle. Three live nodes ⇒ cap 2 ⇒ a 6-segment processor
 * settles 2/2/2; when a node leaves, the survivors' cap rises and they steal its (released or expired)
 * segments so all segments stay covered.
 *
 * <p>Applies to <b>every</b> pooled streaming processor (the {@code users} and {@code ledger}
 * projection groups) via the name-less {@code registerPooledStreamingEventProcessorConfiguration}.
 * In the single-node demo {@code dynamicCap()} returns {@code segments}, so this is a no-op there.
 */
@Configuration
public class AxonProcessorBalancingConfig {

    @Bean
    public ConfigurerModule pooledProcessorSegmentCap(HeartbeatService heartbeat) {
        return configurer -> configurer.eventProcessing()
                .registerPooledStreamingEventProcessorConfiguration(
                        (config, builder) -> builder.maxSegmentProvider(name -> heartbeat.dynamicCap()));
    }
}
