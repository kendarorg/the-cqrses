package org.kendar.pfm.dlq;

import org.axonframework.config.Configuration;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.messaging.deadletter.SequencedDeadLetterProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Operator/test control to drain the projection DLQ: re-run every dead-lettered event at least once
 * through the live handler topology — the Axon equivalent of the kf sample's {@code DlqControl}.
 * Re-running is safe and repeatable (the read model is insert-ignore on {@code op_id}, so a re-applied
 * event can never double-count), which is what lets the flood-consistency IT drain stranded events
 * before its final read-model verdict.
 *
 * <p>For each projection group Axon exposes a {@link SequencedDeadLetterProcessor}; {@code processAny()}
 * re-delivers the next dead-lettered sequence to the live handler and, on success, evicts it. We loop
 * until it reports nothing left. Any node can drain the whole queue — all nodes carry the same
 * projection handlers and write to the same read model.
 */
@Component
public class DlqControl {

    private static final Logger LOGGER = LoggerFactory.getLogger(DlqControl.class);

    private final Configuration axonConfiguration;

    public DlqControl(Configuration axonConfiguration) {
        this.axonConfiguration = axonConfiguration;
    }

    /** Re-run every dead letter once across all projection groups; never aborts on a single failure. */
    public RetryReport retryAll() {
        int resolved = 0;
        int failed = 0;
        for (String group : DlqConfig.DLQ_GROUPS) {
            Optional<SequencedDeadLetterProcessor<EventMessage<?>>> proc =
                    axonConfiguration.eventProcessingConfiguration().sequencedDeadLetterProcessor(group);
            if (proc.isEmpty()) {
                continue;
            }
            SequencedDeadLetterProcessor<EventMessage<?>> p = proc.get();
            boolean any = true;
            while (any) {
                try {
                    any = p.processAny();
                    if (any) {
                        resolved++;
                    }
                } catch (RuntimeException ex) {
                    failed++;
                    LOGGER.warn("dlq retry failed for group {}: {}", group, ex.getMessage());
                    any = false; // move on to the next group; this one gets another attempt next sweep
                }
            }
        }
        RetryReport report = new RetryReport(resolved + failed, resolved, failed);
        if (resolved + failed > 0) {
            LOGGER.trace("dlq retry-all: {}", report);
        }
        return report;
    }

    /** Outcome of a {@link #retryAll()} sweep. Serialised straight to JSON by the controller. */
    public record RetryReport(int attempted, int resolved, int failed) {
    }
}
