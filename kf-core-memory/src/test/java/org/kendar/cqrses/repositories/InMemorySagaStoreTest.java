package org.kendar.cqrses.repositories;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.annotations.SagaHandler;
import org.kendar.cqrses.annotations.SagaId;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.exceptions.InvalidHandlerException;
import org.kendar.cqrses.serialization.JacksonMessageSerializer;
import org.kendar.cqrses.serialization.MessageSerializer;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class InMemorySagaStoreTest {

    private InMemorySagaStore store;
    private MessageSerializer<?,?> serializer;

    @BeforeEach
    void setUp() {
        GlobalRegistry.clear();
        serializer = new JacksonMessageSerializer();
        GlobalRegistry.register(MessageSerializer.class, serializer);
        store = new InMemorySagaStore();
    }

    @AfterEach
    void tearDown() {
        GlobalRegistry.clear();
    }

    // ── test sagas ───────────────────────────────────────────────────────────

    @Test
    void storeAndLoadById_returnsPersistedFields() {
        OrderSaga s = new OrderSaga("saga-1", "ord-1", "cust-1");
        store.storeSaga(s);

        Optional<SagaInstance> loaded = store.loadSaga("saga-1");
        assertTrue(loaded.isPresent());
        assertEquals("saga-1", loaded.get().getId());
        assertEquals("OrderSaga", loaded.get().getType());
        assertNotNull(loaded.get().getContent());

        OrderSaga roundtrip = serializer.deserialize(loaded.get().getContent(), OrderSaga.class);
        assertEquals("saga-1", roundtrip.id);
        assertEquals("ord-1", roundtrip.orderId);
        assertEquals("cust-1", roundtrip.customerId);
    }

    @Test
    void storeSaga_setsCanonicalCorrelationIdToFirstValue() {
        OrderSaga s = new OrderSaga("saga-1", "ord-1", "cust-1");
        store.storeSaga(s);

        SagaInstance instance = store.loadSaga("saga-1").orElseThrow();
        // First @SagaHandler discovered → orderId is the canonical correlation.
        assertEquals("ord-1", instance.getCorrelationId());
    }

    @Test
    void storeSaga_withNoCorrelations_setsNullCorrelationId() {
        store.storeSaga(new NoCorrelationSaga("saga-x"));
        assertNull(store.loadSaga("saga-x").orElseThrow().getCorrelationId());
    }

    @Test
    void loadSagaByNullIdReturnsEmpty() {
        assertTrue(store.loadSaga(null).isEmpty());
    }

    // ── basics ───────────────────────────────────────────────────────────────

    @Test
    void loadUnknownSagaReturnsEmpty() {
        assertTrue(store.loadSaga("missing").isEmpty());
    }

    @Test
    void storeNullSagaThrows() {
        assertThrows(NullPointerException.class, () -> store.storeSaga(null));
    }

    @Test
    void storeSagaWithNullSagaIdValueThrows() {
        OrderSaga s = new OrderSaga(null, "ord-1", "cust-1");
        assertThrows(IllegalStateException.class, () -> store.storeSaga(s));
    }

    @Test
    void storeSagaWithoutSagaIdFieldThrows() {
        assertThrows(InvalidHandlerException.class, () -> store.storeSaga(new NoSagaIdSaga()));
    }

    @Test
    void loadByCorrelationId_returnsSagaUsingTypeSimpleName() {
        store.storeSaga(new OrderSaga("saga-1", "ord-1", "cust-1"));

        Optional<SagaInstance> byOrder = store.loadSagaByCorrelationId("ord-1", "OrderSaga");
        assertTrue(byOrder.isPresent());
        assertEquals("saga-1", byOrder.get().getId());
        assertEquals("ord-1", byOrder.get().getCorrelationId(),
                "view should reflect the value the caller looked up");

        Optional<SagaInstance> byCustomer = store.loadSagaByCorrelationId("cust-1", "OrderSaga");
        assertTrue(byCustomer.isPresent());
        assertEquals("saga-1", byCustomer.get().getId());
        assertEquals("cust-1", byCustomer.get().getCorrelationId(),
                "lookup-by-customerId must reflect customerId in the returned view");
    }

    @Test
    void loadByCorrelationId_unknownValueReturnsEmpty() {
        store.storeSaga(new OrderSaga("saga-1", "ord-1", "cust-1"));
        assertTrue(store.loadSagaByCorrelationId("missing", "OrderSaga").isEmpty());
    }

    @Test
    void loadByCorrelationId_unknownTypeReturnsEmpty() {
        store.storeSaga(new OrderSaga("saga-1", "ord-1", "cust-1"));
        assertTrue(store.loadSagaByCorrelationId("ord-1", "OtherType").isEmpty());
    }

    @Test
    void loadByCorrelationId_nullArgsReturnEmpty() {
        store.storeSaga(new OrderSaga("saga-1", "ord-1", "cust-1"));
        assertTrue(store.loadSagaByCorrelationId(null, "OrderSaga").isEmpty());
        assertTrue(store.loadSagaByCorrelationId("ord-1", null).isEmpty());
        assertTrue(store.loadSagaByCorrelationId(null, null).isEmpty());
    }

    // ── correlation lookups ──────────────────────────────────────────────────

    @Test
    void correlationIndexIsTypeScoped_sameValueDifferentTypes() {
        // Both sagas correlate by "shared-order"; they must not shadow each other.
        store.storeSaga(new OrderSaga("o-1", "shared-order", "cust-1"));
        store.storeSaga(new ShipmentSaga("s-1", "shared-order"));

        assertEquals("o-1",
                store.loadSagaByCorrelationId("shared-order", "OrderSaga").orElseThrow().getId());
        assertEquals("s-1",
                store.loadSagaByCorrelationId("shared-order", "ShipmentSaga").orElseThrow().getId());
    }

    @Test
    void reStore_updatesContent() {
        OrderSaga s = new OrderSaga("saga-1", "ord-1", "cust-1");
        store.storeSaga(s);

        s.state = "updated";
        store.storeSaga(s);

        OrderSaga reloaded = serializer.deserialize(
                store.loadSaga("saga-1").orElseThrow().getContent(), OrderSaga.class);
        assertEquals("updated", reloaded.state);
    }

    @Test
    void reStore_dropsStaleCorrelationValue() {
        OrderSaga s = new OrderSaga("saga-1", "ord-1", "cust-1");
        store.storeSaga(s);
        assertTrue(store.loadSagaByCorrelationId("cust-1", "OrderSaga").isPresent());

        s.customerId = null;
        store.storeSaga(s);

        assertTrue(store.loadSagaByCorrelationId("cust-1", "OrderSaga").isEmpty(),
                "stale correlation value must be dropped from the index on re-store");
        assertTrue(store.loadSagaByCorrelationId("ord-1", "OrderSaga").isPresent(),
                "still-present correlation must remain indexed");
    }

    @Test
    void reStore_changingCorrelationValue_movesIndexToNewBucket() {
        OrderSaga s = new OrderSaga("saga-1", "ord-1", "cust-1");
        store.storeSaga(s);

        s.orderId = "ord-2";
        store.storeSaga(s);

        assertTrue(store.loadSagaByCorrelationId("ord-1", "OrderSaga").isEmpty(),
                "old correlation value must no longer point at the saga");
        assertEquals("saga-1",
                store.loadSagaByCorrelationId("ord-2", "OrderSaga").orElseThrow().getId(),
                "new correlation value must resolve to the saga");
    }

    @Test
    void reStore_returningCorrelationValueIsRestored() {
        OrderSaga s = new OrderSaga("saga-1", "ord-1", "cust-1");
        store.storeSaga(s);

        s.customerId = null;
        store.storeSaga(s);
        assertTrue(store.loadSagaByCorrelationId("cust-1", "OrderSaga").isEmpty());

        s.customerId = "cust-1";
        store.storeSaga(s);
        assertEquals("saga-1",
                store.loadSagaByCorrelationId("cust-1", "OrderSaga").orElseThrow().getId());
    }

    // ── re-store / correlation diff ──────────────────────────────────────────

    @Test
    void multipleSagasOfSameType_areIndependentByPrimaryId() {
        store.storeSaga(new OrderSaga("saga-a", "ord-a", "cust-a"));
        store.storeSaga(new OrderSaga("saga-b", "ord-b", "cust-b"));

        assertEquals("saga-a", store.loadSaga("saga-a").orElseThrow().getId());
        assertEquals("saga-b", store.loadSaga("saga-b").orElseThrow().getId());
        assertEquals("saga-a",
                store.loadSagaByCorrelationId("ord-a", "OrderSaga").orElseThrow().getId());
        assertEquals("saga-b",
                store.loadSagaByCorrelationId("ord-b", "OrderSaga").orElseThrow().getId());
    }

    public static class OrderSaga {
        @SagaId
        public String id;
        public String orderId;
        public String customerId;
        public String state = "";

        public OrderSaga() {
        }

        public OrderSaga(String id, String orderId, String customerId) {
            this.id = id;
            this.orderId = orderId;
            this.customerId = customerId;
        }

        public String getOrderId() {
            return orderId;
        }

        public String getCustomerId() {
            return customerId;
        }

        @SagaHandler(associationProperty = "orderId")
        public void onOrder(Object e) {
        }

        @SagaHandler(associationProperty = "customerId")
        public void onCustomer(Object e) {
        }
    }

    /**
     * Different type, overlapping correlation property names with OrderSaga.
     */
    public static class ShipmentSaga {
        @SagaId
        public String id;
        public String orderId;

        public ShipmentSaga() {
        }

        public ShipmentSaga(String id, String orderId) {
            this.id = id;
            this.orderId = orderId;
        }

        public String getOrderId() {
            return orderId;
        }

        @SagaHandler(associationProperty = "orderId")
        public void onOrder(Object e) {
        }
    }

    public static class NoCorrelationSaga {
        @SagaId
        public String id;

        public NoCorrelationSaga() {
        }

        public NoCorrelationSaga(String id) {
            this.id = id;
        }
    }

    public static class NoSagaIdSaga {
        public String name;
    }
}
