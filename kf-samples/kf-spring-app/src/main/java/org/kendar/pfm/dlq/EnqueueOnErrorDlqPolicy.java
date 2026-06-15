package org.kendar.pfm.dlq;

import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.dlq.DlqEnqueueDecisionResult;
import org.kendar.cqrses.dlq.DlqEnqueuePolicy;

/**
 * Routes a failed projection event to the DLQ instead of silently dropping it. The framework's
 * default event-side policy returns {@link DlqEnqueueDecisionResult#ignore()}, which — combined
 * with the cluster pump advancing its checkpoint after every dispatch (at-least-once) — turns any
 * transient projection-handler failure into permanent, silent read-model data loss: the event is
 * neither re-applied nor recorded anywhere.
 *
 * <p>With this policy the failure is enqueued onto the DLQ; paired with a
 * {@link org.kendar.cqrses.pg.PerAggregateSequencePolicy} the dead letter — and its head-of-line
 * block — is keyed per aggregate, so only the affected aggregate's subsequent events queue up
 * behind it while every other aggregate keeps projecting. A {@code DlqManager} can then re-run the
 * dead letters (the read model is idempotent, insert-ignore on {@code op_id}).
 */
public final class EnqueueOnErrorDlqPolicy extends DlqEnqueuePolicy {

    @Override
    public DlqEnqueueDecisionResult shouldEnqueue(InternalMessage message, Throwable error) {
        return error != null
                ? DlqEnqueueDecisionResult.enqueue()
                : DlqEnqueueDecisionResult.doNotEnqueue();
    }
}
