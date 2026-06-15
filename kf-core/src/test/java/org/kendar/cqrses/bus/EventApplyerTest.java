package org.kendar.cqrses.bus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.annotations.*;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.exceptions.InvalidRegistrationException;
import org.kendar.cqrses.utils.UUIDGenerator;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class EventApplyerTest {

    @BeforeEach
    void setUp() {
        GlobalRegistry.clear();
        // GlobalRegistry.register(Class) auto-subscribes to the registered
        // CommandBus / EventBus. The stubs let us register an aggregate
        // without spinning up a real bus.
        GlobalRegistry.register(CommandBus.class, StubBuses.noopCommandBus());
        GlobalRegistry.register(EventBus.class, StubBuses.noopEventBus());
    }

    @AfterEach
    void tearDown() {
        EventApplyer.drain();    // defensive: clear any leaked buffer
        GlobalRegistry.clear();
    }

    @Test
    void applyFoldsEventOntoAggregateInstance() {
        GlobalRegistry.register(Acc.class);

        Acc acc = new Acc();
        EventApplyer.apply(acc, new Credited(UUIDGenerator.newUuid(), 30));
        EventApplyer.apply(acc, new Credited(UUIDGenerator.newUuid(), 12));

        assertEquals(42, acc.balance);
    }

    @Test
    void applyRecordsEmittedWhenBufferActive() {
        GlobalRegistry.register(Acc.class);
        Acc acc = new Acc();
        Credited e1 = new Credited(UUIDGenerator.newUuid(), 1);
        Credited e2 = new Credited(UUIDGenerator.newUuid(), 2);

        EventApplyer.begin();
        EventApplyer.apply(acc, e1);
        EventApplyer.apply(acc, e2);
        List<Object> drained = EventApplyer.drain();

        assertEquals(List.of(e1, e2), drained);
        assertFalse(EventApplyer.isActive(), "drain must clear the buffer");
    }

    @Test
    void applyDoesNotRecordWhenNoBufferActive() {
        GlobalRegistry.register(Acc.class);
        EventApplyer.apply(new Acc(), new Credited(UUIDGenerator.newUuid(), 5));

        // No begin() was called → drain() returns empty.
        assertTrue(EventApplyer.drain().isEmpty());
    }

    @Test
    void applyThrowsWhenAggregateHasNoHandlerForEventType() {
        GlobalRegistry.register(Acc.class);

        var ex = assertThrows(InvalidRegistrationException.class,
                () -> EventApplyer.apply(new Acc(), new Unknown()));
        assertTrue(ex.getMessage().contains(Acc.class.getName()));
        assertTrue(ex.getMessage().contains(Unknown.class.getName()));
    }

    @Test
    void applyRejectsNullArguments() {
        GlobalRegistry.register(Acc.class);
        assertThrows(IllegalArgumentException.class,
                () -> EventApplyer.apply(null, new Credited(UUIDGenerator.newUuid(), 1)));
        assertThrows(IllegalArgumentException.class,
                () -> EventApplyer.apply(new Acc(), null));
    }

    @Test
    void drainAfterBeginWithoutApplyReturnsEmpty() {
        EventApplyer.begin();
        assertTrue(EventApplyer.drain().isEmpty());
        assertFalse(EventApplyer.isActive());
    }

    // ── fixtures ──────────────────────────────────────────────────────────

    @Aggregate(group = "acc-test")
    public static class Acc {
        public long balance;

        @CommandHandler
        public void handle(Noop ignored) {
        }

        @EventHandler
        public void on(Credited e) {
            balance += e.amount;
        }
    }

    @Command(version = 1)
    public static class Noop {
        @AggregateIdentifier
        public UUID id = UUIDGenerator.newUuid();
    }

    @Event(version = 1)
    public static class Credited {
        @AggregateIdentifier
        public UUID id;
        public long amount;

        public Credited() {
        }

        public Credited(UUID id, long amount) {
            this.id = id;
            this.amount = amount;
        }
    }

    @Event(version = 1)
    public static class Unknown {
        @AggregateIdentifier
        public UUID id = UUIDGenerator.newUuid();
    }
}
