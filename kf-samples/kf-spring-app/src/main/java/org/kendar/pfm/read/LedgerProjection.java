package org.kendar.pfm.read;

import org.kendar.cqrses.annotations.EventHandler;
import org.kendar.cqrses.annotations.Projection;
import org.kendar.pfm.domain.events.OperationRecorded;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Projects {@code OperationRecorded} into the durable {@code pfm_operation} read table (insert-ignore
 * on op_id → idempotent). A live Spring bean bridged via {@code GlobalRegistry.register(class, bean)};
 * stateless, so concurrent invocation across lanes is safe.
 */
@Component
@Projection(group = "ledger")
public class LedgerProjection {

    private static final Logger LOGGER = LoggerFactory.getLogger(LedgerProjection.class);

    @EventHandler
    public void on(OperationRecorded e, OperationReadStore store) {
        store.record(e.opId, e.userId, e.type, e.amount, e.tag, e.epochMillis);
        LOGGER.trace("ledger projected opId={} userId={} type={} amount={} tag={}",
                e.opId, e.userId, e.type, e.amount, e.tag);
    }
}
