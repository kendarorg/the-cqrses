package org.kendar.pfm.read;

import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.kendar.pfm.domain.events.OperationRecorded;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Projects {@code OperationRecorded} into the durable {@code pfm_operation} read table (insert-ignore
 * on op_id → idempotent). The {@code "ledger"} {@link ProcessingGroup} is a pooled streaming event
 * processor distributed across nodes by token-store segment claiming. Stateless → safe under
 * concurrent segment dispatch.
 */
@Component
@ProcessingGroup("ledger")
public class LedgerProjection {

    private static final Logger LOGGER = LoggerFactory.getLogger(LedgerProjection.class);

    private final OperationReadStore store;

    public LedgerProjection(OperationReadStore store) {
        this.store = store;
    }

    @EventHandler
    public void on(OperationRecorded e) {
        store.record(e.opId, e.userId, e.type, e.amount, e.tag, e.epochMillis);
        LOGGER.debug("ledger projected opId={} userId={} type={} amount={} tag={}",
                e.opId, e.userId, e.type, e.amount, e.tag);
    }
}
