package org.kendar.cqrses.dlq.evolved;

import org.junit.jupiter.api.Test;
import org.kendar.cqrses.dlq.DlqEnqueueDecision;
import org.kendar.cqrses.dlq.DlqEnqueueDecisionResult;

import static org.junit.jupiter.api.Assertions.*;

class DlqEnqueueDecisionResultTest {

    @Test
    void enqueueDecisionOnlyReportsEnqueue() {
        var r = DlqEnqueueDecisionResult.enqueue();
        assertEquals(DlqEnqueueDecision.ENQUEUE, r.getDecision());
        assertTrue(r.shouldEnqueue());
        assertFalse(r.shouldEvict());
        assertFalse(r.shouldIgnore());
    }

    @Test
    void requeueAlsoCountsAsEnqueue() {
        var r = DlqEnqueueDecisionResult.requeue();
        assertEquals(DlqEnqueueDecision.REQUEUE, r.getDecision());
        assertTrue(r.shouldEnqueue());
        assertFalse(r.shouldEvict());
        assertFalse(r.shouldIgnore());
    }

    @Test
    void evictDecisionOnlyReportsEvict() {
        var r = DlqEnqueueDecisionResult.evict();
        assertEquals(DlqEnqueueDecision.EVICT, r.getDecision());
        assertFalse(r.shouldEnqueue());
        assertTrue(r.shouldEvict());
        assertFalse(r.shouldIgnore());
    }

    @Test
    void ignoreDecisionOnlyReportsIgnore() {
        var r = DlqEnqueueDecisionResult.ignore();
        assertEquals(DlqEnqueueDecision.IGNORE, r.getDecision());
        assertFalse(r.shouldEnqueue());
        assertFalse(r.shouldEvict());
        assertTrue(r.shouldIgnore());
    }

    @Test
    void doNotEnqueueDecisionReportsNothing() {
        var r = DlqEnqueueDecisionResult.doNotEnqueue();
        assertEquals(DlqEnqueueDecision.DO_NOT_ENQUEUE, r.getDecision());
        assertFalse(r.shouldEnqueue());
        assertFalse(r.shouldEvict());
        assertFalse(r.shouldIgnore());
    }

    @Test
    void constructorRetainsSuppliedDecision() {
        var r = new DlqEnqueueDecisionResult(DlqEnqueueDecision.EVICT);
        assertEquals(DlqEnqueueDecision.EVICT, r.getDecision());
        assertTrue(r.shouldEvict());
    }

    @Test
    void decisionEnumHasExpectedValues() {
        assertArrayEquals(
                new DlqEnqueueDecision[]{
                        DlqEnqueueDecision.DO_NOT_ENQUEUE,
                        DlqEnqueueDecision.ENQUEUE,
                        DlqEnqueueDecision.REQUEUE,
                        DlqEnqueueDecision.EVICT,
                        DlqEnqueueDecision.IGNORE
                },
                DlqEnqueueDecision.values());
    }
}
