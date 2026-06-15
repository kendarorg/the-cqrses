package org.kendar.cqrses.repositories;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.bus.Context;
import org.kendar.cqrses.db.AbstractJdbcTest;
import org.kendar.cqrses.dlq.DlqItem;
import org.kendar.cqrses.dlq.DlqItemStatus;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Same behavioural suite as {@code InMemoryDlqStoreTest}, run against the JDBC
 * store — proves FIFO parity (order survives {@code updateItem}, remove from any
 * position, head-of-line eviction).
 */
class JdbcDlqStoreTest extends AbstractJdbcTest {

    private JdbcDlqStore store;

    @BeforeEach
    void setUp() {
        store = new JdbcDlqStore(db);
    }

    private DlqItem item(String seq) {
        var i = new DlqItem();
        i.setId(UUID.randomUUID());
        i.setSequenceId(seq);
        i.setProcessingGroup("g");
        i.setStatus(DlqItemStatus.PENDING);
        return i;
    }

    private List<UUID> idsOf(String seq) {
        return store.listItems(seq).stream().map(DlqItem::getId).toList();
    }

    @Test
    void removeItemRemovesFromQueueAndIdIndex() {
        var a = item("s");
        var b = item("s");
        store.addItem(a, "s");
        store.addItem(b, "s");

        store.removeItem(a.getId());

        assertTrue(store.getItem(a.getId()).isEmpty(), "gone from id index");
        assertEquals(List.of(b.getId()), idsOf("s"), "gone from the sequence queue");
        assertTrue(store.hasBlockedItems("s"), "b still blocks");
    }

    @Test
    void removeItemCanTargetANonHeadItem() {
        var a = item("s");
        var b = item("s");
        var c = item("s");
        store.addItem(a, "s");
        store.addItem(b, "s");
        store.addItem(c, "s");

        store.removeItem(b.getId()); // middle

        assertEquals(List.of(a.getId(), c.getId()), idsOf("s"), "order preserved, middle removed");
    }

    @Test
    void removeItemUnknownIdIsNoOp() {
        store.addItem(item("s"), "s");
        assertDoesNotThrow(() -> store.removeItem(UUID.randomUUID()));
        assertEquals(1, store.listItems("s").size());
    }

    @Test
    void removeLastItemClearsTheBlock() {
        var a = item("s");
        store.addItem(a, "s");
        store.removeItem(a.getId());
        assertFalse(store.hasBlockedItems("s"));
    }

    @Test
    void updateItemPreservesFifoPosition() {
        var a = item("s");
        var b = item("s");
        var c = item("s");
        store.addItem(a, "s");
        store.addItem(b, "s");
        store.addItem(c, "s");

        a.setRetryCount(1);
        store.updateItem(a);

        assertEquals(List.of(a.getId(), b.getId(), c.getId()), idsOf("s"));
        assertEquals(1, store.getItem(a.getId()).orElseThrow().getRetryCount());
    }

    @Test
    void evictFirstRemovesHeadOfLine() {
        var a = item("s");
        var b = item("s");
        store.addItem(a, "s");
        store.addItem(b, "s");

        store.evictFirst("s");

        assertEquals(List.of(b.getId()), idsOf("s"), "FIFO head evicted");
        assertTrue(store.getItem(a.getId()).isEmpty());
    }

    @Test
    void updateStatusChangesOnlyStatus() {
        var a = item("s");
        store.addItem(a, "s");
        store.updateStatus(a.getId(), DlqItemStatus.RESOLVED);
        assertEquals(DlqItemStatus.RESOLVED, store.getItem(a.getId()).orElseThrow().getStatus());
    }

    @Test
    void getItemRoundTripsAllFields() {
        var a = item("s");
        a.setEventType("com.x.Evt");
        a.setAggregateId(UUID.randomUUID());
        a.setSerializedEvent(new byte[]{4, 5, 6});
        a.setRetryCount(2);
        a.setErrorMessage("boom");
        a.setErrorClass("java.lang.RuntimeException");
        store.addItem(a, "s");

        DlqItem loaded = store.getItem(a.getId()).orElseThrow();
        assertEquals("com.x.Evt", loaded.getEventType());
        assertEquals(a.getAggregateId(), loaded.getAggregateId());
        assertArrayEquals(new byte[]{4, 5, 6}, loaded.getSerializedEvent());
        assertEquals(2, loaded.getRetryCount());
        assertEquals("boom", loaded.getErrorMessage());
        assertEquals("g", loaded.getProcessingGroup());
    }

    @Test
    void clearEmptiesEverything() {
        store.addItem(item("s"), "s");
        store.addItem(item("t"), "t");
        store.clear();
        assertFalse(store.hasBlockedItems("s"));
        assertFalse(store.hasBlockedItems("t"));
    }

    @Test
    void processingContextWithInstantRoundTripsThroughTheRow() {
        // The processing Context carries an Instant timestamp; it must serialize
        // through the registered MessageSerializer (JSON + JSR-310), not blow up.
        // A FRESH store over the same DB bypasses the identity cache and forces a
        // genuine deserialize from the persisted row.
        var a = item("ctx");
        Context ctx = new Context();
        UUID agg = UUID.randomUUID();
        ctx.setAggregateId(agg);
        ctx.setType("com.x.Evt");
        ctx.setProcessingGroup("g");
        ctx.setTimestamp(Instant.ofEpochMilli(1_700_000_000_123L));
        ctx.getMetadata().put("k", "v");
        a.setProcessingContext(ctx);
        store.addItem(a, "ctx");

        JdbcDlqStore fresh = new JdbcDlqStore(db);
        Context loaded = fresh.getItem(a.getId()).orElseThrow().getProcessingContext();
        assertNotNull(loaded, "processing context must survive the row round-trip");
        assertEquals(agg, loaded.getAggregateId());
        assertEquals("com.x.Evt", loaded.getType());
        assertEquals("g", loaded.getProcessingGroup());
        assertEquals(Instant.ofEpochMilli(1_700_000_000_123L), loaded.getTimestamp());
        assertEquals("v", loaded.getMetadata().get("k"));
    }
}
