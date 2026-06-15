package org.kendar.pfm.dlq;

import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.config.ConfigurerModule;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.deadletter.jpa.JpaSequencedDeadLetterQueue;
import org.axonframework.messaging.deadletter.Decisions;
import org.kendar.pfm.metrics.KfMeters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Gives the projection processing groups a real <b>dead-letter queue</b> instead of letting a throwing
 * handler drop the event — the Axon equivalent of the kf sample's {@code EnqueueOnErrorDlqPolicy} +
 * {@code PerAggregateSequencePolicy}.
 *
 * <p>Why it's needed (identical to kf): a streaming processor advances its token after dispatch
 * (at-least-once), so a projection event whose handler throws — e.g. a transient row-lock timeout
 * under flood + rebalance — would otherwise be skipped forever. Axon's {@code SequencedDeadLetterQueue}
 * captures the failure <b>sequenced per aggregate</b> (the dead letter and its head-of-line block are
 * keyed by the event's sequencing identifier = the aggregate id), so unaffected aggregates keep
 * projecting in order, and {@code /dlq/retry-all} can re-run the dead letters later (idempotent,
 * insert-ignore on {@code op_id}).
 *
 * <p>The default enqueue policy is also overridden per group to increment the {@code kf.dlq.enqueue}
 * meter (kf parity) before enqueuing the failure.
 */
@Configuration
public class DlqConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(DlqConfig.class);

    /** The projection processing groups that get a per-aggregate DLQ. */
    static final List<String> DLQ_GROUPS = List.of("users", "ledger");

    @Bean
    public ConfigurerModule kfDeadLetterQueues(ObjectProvider<KfMeters> meters) {
        return configurer -> {
            var ep = configurer.eventProcessing();
            for (String group : DLQ_GROUPS) {
                ep.registerDeadLetterQueue(group, cfg ->
                        JpaSequencedDeadLetterQueue.<EventMessage<?>>builder()
                                .processingGroup(group)
                                .entityManagerProvider(cfg.getComponent(EntityManagerProvider.class))
                                .transactionManager(cfg.getComponent(TransactionManager.class))
                                .serializer(cfg.serializer())
                                .eventSerializer(cfg.eventSerializer())
                                .genericSerializer(cfg.serializer())
                                .build());
                // Per-group enqueue policy: count the dead-letter (kf.dlq.enqueue) then enqueue it.
                ep.registerDeadLetterPolicy(group, cfg -> (deadLetter, cause) -> {
                    KfMeters m = meters.getIfAvailable();
                    if (m != null) {
                        m.onDlqEnqueued(group, deadLetter.message().getPayloadType().getSimpleName());
                    }
                    return Decisions.enqueue(cause);
                });
                LOGGER.trace("projection group '{}': per-aggregate dead-letter queue enabled", group);
            }
        };
    }
}
