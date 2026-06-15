package org.kendar.cqrses.dlq;

public class DlqEnqueueDecisionResult {
    private final DlqEnqueueDecision decision;

    public DlqEnqueueDecisionResult(DlqEnqueueDecision decision) {
        this.decision = decision;
    }

    public boolean shouldEnqueue(){
        return decision == DlqEnqueueDecision.ENQUEUE || decision == DlqEnqueueDecision.REQUEUE;
    }

    public boolean shouldEvict(){
        return decision == DlqEnqueueDecision.EVICT;
    }

    public boolean shouldIgnore(){
        return decision == DlqEnqueueDecision.IGNORE;
    }

    public static DlqEnqueueDecisionResult doNotEnqueue(){
        return new DlqEnqueueDecisionResult(DlqEnqueueDecision.DO_NOT_ENQUEUE);
    }

    public static DlqEnqueueDecisionResult enqueue(){
        return new DlqEnqueueDecisionResult(DlqEnqueueDecision.ENQUEUE);
    }


    public static DlqEnqueueDecisionResult requeue(){
        return new DlqEnqueueDecisionResult(DlqEnqueueDecision.REQUEUE);
    }

    public static DlqEnqueueDecisionResult evict(){
        return new DlqEnqueueDecisionResult(DlqEnqueueDecision.EVICT);
    }


    public static DlqEnqueueDecisionResult ignore(){
        return new DlqEnqueueDecisionResult(DlqEnqueueDecision.IGNORE);
    }

    public DlqEnqueueDecision getDecision() {
        return decision;
    }
}
