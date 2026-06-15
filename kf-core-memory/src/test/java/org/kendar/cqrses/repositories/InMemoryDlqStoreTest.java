package org.kendar.cqrses.repositories;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.dlq.DlqItem;
import org.kendar.cqrses.dlq.DlqItemStatus;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryDlqStoreTest {

    private InMemoryDlqStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryDlqStore();
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

        // Bump the head (a) — it must NOT move to the tail.
        a.setRetryCount(1);
        store.updateItem(a);

        assertEquals(List.of(a.getId(), b.getId(), c.getId()), idsOf("s"));
        assertEquals(1, store.getItem(a.getId()).orElseThrow().getRetryCount());
    }
}
