package org.kendar.cqrses.dlq;

public enum DlqEnqueueDecision {
    DO_NOT_ENQUEUE,
    ENQUEUE,
    REQUEUE,
    EVICT,
    IGNORE
}
