package org.kendar.cqrses.dlq;

import org.kendar.cqrses.bus.InternalMessage;

public abstract class DlqEnqueuePolicy {
    public DlqEnqueueDecisionResult shouldEnqueue(InternalMessage message){
        return shouldEnqueue(message,null);
    }
    public abstract DlqEnqueueDecisionResult shouldEnqueue(InternalMessage message, Throwable error);

}
