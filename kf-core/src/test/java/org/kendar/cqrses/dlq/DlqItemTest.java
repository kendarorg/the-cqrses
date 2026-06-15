package org.kendar.cqrses.dlq;

import org.junit.jupiter.api.Test;
import org.kendar.cqrses.bus.Context;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class DlqItemTest {

    private static DlqItem item(UUID id, String sequenceId, String processingGroup) {
        var i = new DlqItem();
        i.setId(id);
        i.setSequenceId(sequenceId);
        i.setProcessingGroup(processingGroup);
        return i;
    }

    @Test
    void equalsAndHashCodeUseIdSequenceAndGroupOnly() {
        UUID id = UUID.randomUUID();
        var a = item(id, "seq", "grp");
        var b = item(id, "seq", "grp");
        // Differing non-identity fields must not break equality.
        a.setErrorMessage("first");
        b.setErrorMessage("second");
        b.setRetryCount(7);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void notEqualWhenIdDiffers() {
        assertNotEquals(item(UUID.randomUUID(), "seq", "grp"),
                item(UUID.randomUUID(), "seq", "grp"));
    }

    @Test
    void notEqualWhenSequenceDiffers() {
        UUID id = UUID.randomUUID();
        assertNotEquals(item(id, "seqA", "grp"), item(id, "seqB", "grp"));
    }

    @Test
    void notEqualWhenProcessingGroupDiffers() {
        UUID id = UUID.randomUUID();
        assertNotEquals(item(id, "seq", "grpA"), item(id, "seq", "grpB"));
    }

    @Test
    void notEqualToNullOrOtherType() {
        var a = item(UUID.randomUUID(), "seq", "grp");
        assertNotEquals(a, null);
        assertNotEquals(a, "not a dlq item");
    }

    @Test
    void equalToItself() {
        var a = item(UUID.randomUUID(), "seq", "grp");
        assertEquals(a, a);
    }

    @Test
    void gettersReturnSetValues() {
        var i = new DlqItem();
        UUID id = UUID.randomUUID();
        UUID agg = UUID.randomUUID();
        Instant failed = Instant.now();
        Instant retried = failed.plusSeconds(5);
        byte[] payload = {1, 2, 3};
        var ctx = new Context();

        i.setId(id);
        i.setSequenceId("seq");
        i.setProcessingGroup("grp");
        i.setSerializedEvent(payload);
        i.setEventType("EventX");
        i.setAggregateId(agg);
        i.setProcessingContext(ctx);
        i.setErrorMessage("err");
        i.setErrorClass("java.lang.RuntimeException");
        i.setStackTrace("trace");
        i.setFailedAt(failed);
        i.setRetryCount(3);
        i.setStatus(DlqItemStatus.RETRYING);
        i.setLastRetryErrorMessage("lerr");
        i.setLastRetryErrorClass("lcls");
        i.setLastRetryStackTrace("ltrace");
        i.setLastRetryAt(retried);
        Object originalEvent = new Object();
        i.setOriginalEvent(originalEvent);

        assertEquals(id, i.getId());
        assertEquals("seq", i.getSequenceId());
        assertEquals("grp", i.getProcessingGroup());
        assertArrayEquals(payload, i.getSerializedEvent());
        assertEquals("EventX", i.getEventType());
        assertEquals(agg, i.getAggregateId());
        assertSame(ctx, i.getProcessingContext());
        assertEquals("err", i.getErrorMessage());
        assertEquals("java.lang.RuntimeException", i.getErrorClass());
        assertEquals("trace", i.getStackTrace());
        assertEquals(failed, i.getFailedAt());
        assertEquals(3, i.getRetryCount());
        assertEquals(DlqItemStatus.RETRYING, i.getStatus());
        assertEquals("lerr", i.getLastRetryErrorMessage());
        assertEquals("lcls", i.getLastRetryErrorClass());
        assertEquals("ltrace", i.getLastRetryStackTrace());
        assertEquals(retried, i.getLastRetryAt());
        assertSame(originalEvent, i.getOriginalEvent());
    }

    @Test
    void statusEnumHasExpectedValues() {
        assertArrayEquals(
                new DlqItemStatus[]{
                        DlqItemStatus.PENDING,
                        DlqItemStatus.RETRYING,
                        DlqItemStatus.RESOLVED,
                        DlqItemStatus.DISMISSED
                },
                DlqItemStatus.values());
    }
}
