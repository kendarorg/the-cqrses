package org.kendar.cqrses.bus;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.annotations.*;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.exceptions.InvalidRegistrationException;
import org.kendar.cqrses.repositories.EventStore;
import org.kendar.cqrses.repositories.InMemoryDlqStore;
import org.kendar.cqrses.repositories.InMemoryEventStore;
import org.kendar.cqrses.serialization.JacksonMessageSerializer;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.serialization.Upcaster;
import org.kendar.cqrses.serialization.UpcastersManager;
import org.kendar.cqrses.utils.UUIDGenerator;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Full-stack command-side tests for the three Axon-parity features:
 * {@code sendSync} returning the handler's result, the
 * {@code @Aggregate(snapshotEvery)} automatic snapshot trigger, and
 * schema-versioned snapshot upcasting ({@code @Aggregate(version)}).
 */
class SendSyncResultAndSnapshotTest {

    private JacksonMessageSerializer serializer;
    private InMemoryEventStore eventStore;
    private InMemoryCommandBus commandBus;

    /** Setup ritual; upcasters must be registered before the store is built. */
    private void wire(Class<?> aggregateClass, Upcaster... upcasters) {
        GlobalRegistry.clear();
        serializer = new JacksonMessageSerializer();
        GlobalRegistry.register(MessageSerializer.class, serializer);
        GlobalRegistry.register(UpcastersManager.class,
                new UpcastersManager(serializer, List.of(upcasters)));
        eventStore = new InMemoryEventStore();
        GlobalRegistry.register(EventStore.class, eventStore);
        commandBus = new InMemoryCommandBus(serializer, eventStore, new InMemoryDlqStore());
        GlobalRegistry.register(CommandBus.class, commandBus);
        GlobalRegistry.register(EventBus.class, noopEventBus());
        GlobalRegistry.register(aggregateClass);
        commandBus.start();
    }

    @AfterEach
    void tearDown() {
        try {
            if (commandBus != null) commandBus.stop();
        } catch (Exception ignored) {
        }
        GlobalRegistry.clear();
    }

    // ── sendSync result ──────────────────────────────────────────────────────

    @Test
    void sendSyncReturnsTheHandlerResult() {
        wire(CartAggregate.class);
        UUID id = UUIDGenerator.newUuid();

        Integer first = commandBus.sendSync(new AddItem(id, 5));
        Integer second = commandBus.sendSync(new AddItem(id, 7));

        assertEquals(5, first, "handler returns the folded total");
        assertEquals(12, second, "second command sees the rehydrated state");
    }

    @Test
    void sendSyncReturnsNullForVoidHandlers() {
        wire(CartAggregate.class);
        UUID id = UUIDGenerator.newUuid();
        commandBus.sendSync(new AddItem(id, 5));

        Object result = commandBus.sendSync(new ClearCart(id));
        assertNull(result);
    }

    // ── automatic snapshotting ───────────────────────────────────────────────

    @Test
    void autoSnapshotFiresWhenTheStreamCrossesTheThreshold() {
        wire(CartAggregate.class);
        UUID id = UUIDGenerator.newUuid();

        commandBus.sendSync(new AddItem(id, 1));
        commandBus.sendSync(new AddItem(id, 2));
        assertTrue(eventStore.loadSnapshot(id).isEmpty(),
                "two events < snapshotEvery=3: no snapshot yet");

        commandBus.sendSync(new AddItem(id, 3));
        var snap = eventStore.loadSnapshot(id).orElseThrow();
        assertEquals(2, snap.getAggregateVersion(), "stamped with the batch's last 0-based version");
        assertEquals(2, snap.getSchemaVersion(), "stamped with @Aggregate.version");
        assertEquals("CartSnapshot", snap.getSnapshotType());

        CartSnapshot payload = serializer.deserialize(snap.getSnapshot(), CartSnapshot.class);
        assertEquals(6, payload.total);

        assertEquals(6, eventStore.loadAggregate(id, CartAggregate.class).orElseThrow().total,
                "rehydration from snapshot + empty tail matches the folded state");
    }

    @Test
    void autoSnapshotRefreshesAtEveryBoundary() {
        wire(CartAggregate.class);
        UUID id = UUIDGenerator.newUuid();
        for (int i = 1; i <= 6; i++) {
            commandBus.sendSync(new AddItem(id, i));
        }
        var snap = eventStore.loadSnapshot(id).orElseThrow();
        assertEquals(5, snap.getAggregateVersion(), "refreshed at the second boundary");
        assertEquals(21, serializer.deserialize(snap.getSnapshot(), CartSnapshot.class).total);
    }

    @Test
    void autoSnapshotTriggersWhenAMultiEventBatchJumpsPastTheBoundary() {
        wire(CartAggregate.class);
        UUID id = UUIDGenerator.newUuid();
        commandBus.sendSync(new AddItem(id, 1));                  // version 0
        commandBus.sendSync(new AddItemTwice(id, 10));            // versions 1,2 — crosses 3
        var snap = eventStore.loadSnapshot(id).orElseThrow();
        assertEquals(2, snap.getAggregateVersion());
        assertEquals(21, serializer.deserialize(snap.getSnapshot(), CartSnapshot.class).total);
    }

    @Test
    void snapshotEveryWithoutTheAccessorPairFailsAtRegistration() {
        GlobalRegistry.clear();
        serializer = new JacksonMessageSerializer();
        GlobalRegistry.register(MessageSerializer.class, serializer);
        GlobalRegistry.register(UpcastersManager.class, new UpcastersManager(serializer, List.of()));
        var ex = assertThrows(InvalidRegistrationException.class,
                () -> GlobalRegistry.register(BrokenSnapshotAggregate.class));
        assertTrue(ex.getMessage().contains("getSnapshot"));
    }

    // ── snapshot upcasting ───────────────────────────────────────────────────

    @Test
    void staleSnapshotIsUpcastToTheCurrentAggregateVersion() {
        wire(CartAggregate.class, new CartSnapshotUpcaster());
        UUID id = UUIDGenerator.newUuid();
        commandBus.sendSync(new AddItem(id, 4));
        commandBus.sendSync(new AddItem(id, 5));

        // A revision-1 era snapshot: old shape ("sum"), explicitly stamped schema
        // version 1, covering both stored events. The deliberately wrong total
        // proves the loader used the snapshot rather than silently replaying.
        eventStore.storeSnapshot(id, new CartSnapshotOld(100), 1L, 1L);

        var loaded = eventStore.loadAggregate(id, CartAggregate.class).orElseThrow();
        assertEquals(100, loaded.total, "upcast snapshot applied; no replay of covered events");

        commandBus.sendSync(new AddItem(id, 7));
        assertEquals(107, eventStore.loadAggregate(id, CartAggregate.class).orElseThrow().total,
                "post-snapshot tail still replays on top of the upcast state");
    }

    @Test
    void staleSnapshotWithoutAnUpcasterIsDiscardedAndTheStreamReplays() {
        wire(CartAggregate.class); // no upcasters registered
        UUID id = UUIDGenerator.newUuid();
        commandBus.sendSync(new AddItem(id, 4));
        commandBus.sendSync(new AddItem(id, 5));
        eventStore.storeSnapshot(id, new CartSnapshotOld(100), 1L, 1L);

        var loaded = eventStore.loadAggregate(id, CartAggregate.class).orElseThrow();
        assertEquals(9, loaded.total,
                "irreparably stale snapshot discarded: full replay yields the true state");
    }

    @Test
    void currentVersionSnapshotPassesThroughUntouched() {
        wire(CartAggregate.class);
        UUID id = UUIDGenerator.newUuid();
        for (int i = 1; i <= 3; i++) commandBus.sendSync(new AddItem(id, i));
        // Auto-snapshot stored at schema version 2 == @Aggregate.version → used as-is.
        assertEquals(6, eventStore.loadAggregate(id, CartAggregate.class).orElseThrow().total);
    }

    // ── fixtures ─────────────────────────────────────────────────────────────

    @Command(version = 1)
    public static class AddItem {
        @AggregateIdentifier
        public UUID cartId;
        public int qty;

        public AddItem() {
        }

        public AddItem(UUID cartId, int qty) {
            this.cartId = cartId;
            this.qty = qty;
        }
    }

    @Command(version = 1)
    public static class AddItemTwice {
        @AggregateIdentifier
        public UUID cartId;
        public int qty;

        public AddItemTwice() {
        }

        public AddItemTwice(UUID cartId, int qty) {
            this.cartId = cartId;
            this.qty = qty;
        }
    }

    @Command(version = 1)
    public static class ClearCart {
        @AggregateIdentifier
        public UUID cartId;

        public ClearCart() {
        }

        public ClearCart(UUID cartId) {
            this.cartId = cartId;
        }
    }

    @Event(version = 1)
    public static class ItemAdded {
        @AggregateIdentifier
        public UUID cartId;
        public int qty;

        public ItemAdded() {
        }

        public ItemAdded(UUID cartId, int qty) {
            this.cartId = cartId;
            this.qty = qty;
        }
    }

    @Event(version = 1)
    public static class CartCleared {
        @AggregateIdentifier
        public UUID cartId;

        public CartCleared() {
        }

        public CartCleared(UUID cartId) {
            this.cartId = cartId;
        }
    }

    /** Current (revision-2) snapshot shape. */
    public static class CartSnapshot {
        public int total;

        public CartSnapshot() {
        }

        public CartSnapshot(int total) {
            this.total = total;
        }
    }

    /** Revision-1 era snapshot shape: the field was called {@code sum}. */
    public static class CartSnapshotOld {
        public int sum;

        public CartSnapshotOld() {
        }

        public CartSnapshotOld(int sum) {
            this.sum = sum;
        }
    }

    /** One hop, 1 → 2: rename {@code sum} to {@code total}. */
    public static class CartSnapshotUpcaster implements Upcaster {
        @UpcasterSpec(origin = "CartSnapshotOld", from = 1, to = 2)
        public JsonNode upcast(JsonNode node) {
            ObjectNode obj = (ObjectNode) node;
            obj.set("total", obj.remove("sum"));
            return obj;
        }
    }

    @Aggregate(group = "carts", version = 2, snapshotEvery = 3)
    public static class CartAggregate {
        public int total;

        @CommandHandler
        public Integer handle(AddItem cmd) {
            EventApplyer.apply(this, new ItemAdded(cmd.cartId, cmd.qty));
            return total;
        }

        @CommandHandler
        public Integer handle(AddItemTwice cmd) {
            EventApplyer.apply(this, new ItemAdded(cmd.cartId, cmd.qty));
            EventApplyer.apply(this, new ItemAdded(cmd.cartId, cmd.qty));
            return total;
        }

        @CommandHandler
        public void handle(ClearCart cmd) {
            EventApplyer.apply(this, new CartCleared(cmd.cartId));
        }

        @EventHandler
        public void on(ItemAdded e) {
            total += e.qty;
        }

        @EventHandler
        public void on(CartCleared e) {
            total = 0;
        }

        public CartSnapshot getSnapshot() {
            return new CartSnapshot(total);
        }

        public void setSnapshot(CartSnapshot snap) {
            this.total = snap.total;
        }
    }

    /** snapshotEvery without the getSnapshot()/setSnapshot pair — must fail registration. */
    @Aggregate(group = "broken", snapshotEvery = 2)
    public static class BrokenSnapshotAggregate {
        @CommandHandler
        public void handle(AddItem cmd) {
        }
    }

    private static EventBus noopEventBus() {
        return new EventBus(new JacksonMessageSerializer(), null) {
            @Override
            public Object findTarget(Object event, Registration registration) {
                return null;
            }

            @Override
            public void start() {
            }

            @Override
            public void stop() {
            }

            @Override
            public void clear() {
            }

            @Override
            void send(Object event, Context context) {
            }

            @Override
            protected boolean registerInternal(Class<?> handlerClass) {
                return false;
            }
        };
    }
}
