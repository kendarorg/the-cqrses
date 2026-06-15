package org.kendar.pfm.dlq;

import org.kendar.cqrses.bus.Bus;
import org.kendar.cqrses.pg.PerAggregateSequencePolicy;
import org.kendar.cqrses.spring.EventBusCustomizer;
import org.kendar.cqrses.spring.KfProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Gives the event-side projection groups ({@code kf.processing-groups}) a real DLQ policy instead of
 * the starter's default {@code ignore()}: on a handler failure the event is enqueued to the DLQ,
 * keyed <b>per aggregate</b> ({@link PerAggregateSequencePolicy}).
 *
 * <p>Why this is needed: the cluster event-side pump ({@code SegmentProcessor}) advances its durable
 * checkpoint after every dispatch (at-least-once), so a projection event whose handler throws — e.g.
 * a transient row-lock timeout under heavy flood + rebalance — would otherwise be skipped forever
 * (the checkpoint has moved past it and nothing re-applies it). Enqueuing per aggregate captures the
 * failure for a later re-run while letting unaffected aggregates keep projecting in order.
 *
 * <p>The customizer is applied to the auto-configured {@code EventBus} before its topology freezes,
 * so the lanes built at start-up pick up this policy (see {@code ProcessingGroupsManager#start}).
 */
@Configuration
public class ProjectionDlqPolicyConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProjectionDlqPolicyConfig.class);

    @Bean
    public EventBusCustomizer perAggregateProjectionDlq(KfProperties properties) {
        return eventBus -> {
            for (String group : properties.getProcessingGroups()) {
                eventBus.setProcessingGroupPolicy(new Bus.ProcessingGroupPolicyConfig(
                        group, new EnqueueOnErrorDlqPolicy(), new PerAggregateSequencePolicy()));
                LOGGER.trace("projection group '{}': DLQ-on-error, head-of-line blocked per aggregate", group);
            }
        };
    }
}
