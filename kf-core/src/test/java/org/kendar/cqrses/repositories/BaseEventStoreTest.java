package org.kendar.cqrses.repositories;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.annotations.*;
import org.kendar.cqrses.bus.*;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.exceptions.MissingAggregateConstructorException;
import org.kendar.cqrses.exceptions.MissingAggregateSnapshotHandlerException;
import org.kendar.cqrses.serialization.JacksonMessageSerializer;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.serialization.UpcastersManager;
import org.kendar.cqrses.utils.UUIDGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class BaseEventStoreTest {

    private JacksonMessageSerializer serializer;
    private FixtureStore store;
    private UpcastersManager upcasterManager;

    @BeforeEach
    void setUp() {
        GlobalRegistry.clear();
        serializer = new JacksonMessageSerializer();
        upcasterManager = new UpcastersManager(serializer,List.of());
        GlobalRegistry.register(UpcastersManager.class, upcasterManager);
        // The fixture aggregate has @EventHandler methods, so registering it
        // triggers autoSubscribe — wire bus stubs to keep that NPE-free.
        GlobalRegistry.register(CommandBus.class, StubBuses.noopCommandBus());
        GlobalRegistry.register(EventBus.class, StubBuses.noopEventBus());
        GlobalRegistry.register(MessageSerializer.class, serializer);
        GlobalRegistry.register(Counter.class);
        store = new FixtureStore();
    }

    @AfterEach
    void tearDown() {
        GlobalRegistry.clear();
    }

    private InternalMessage wrap(Object event, UUID aggregateId, long aggregateVersion) {
        InternalMessage m = new InternalMessage();
        m.setEvent(true);
        m.setPayload(serializer.serialize(event));
        Context ctx = new Context();
        ctx.setAggregateId(aggregateId);
        ctx.setAggregateVersion(aggregateVersion);
        ctx.setType(event.getClass().getSimpleName());
        ctx.setVersion(1L);
        m.setContext(ctx);
        return m;
    }

    @Test
    void emptyEventStreamReturnsBareInstance() {
        Optional<Counter> opt = store.loadAggregate(UUIDGenerator.newUuid(), Counter.class);
        assertTrue(opt.isPresent());
        assertEquals(0, opt.get().value);
    }

    @Test
    void singleEventIsAppliedAndMutatesAggregate() {
        UUID id = UUIDGenerator.newUuid();
        store.events.add(wrap(new Incremented(id, 5), id, 0));

        Counter c = store.loadAggregate(id, Counter.class).orElseThrow();
        assertEquals(5, c.value);
    }

    @Test
    void outOfOrderEventsAreSortedByAggregateVersionBeforeApply() {
        UUID id = UUIDGenerator.newUuid();
        // Insert with intentionally-shuffled versions.
        store.events.add(wrap(new Incremented(id, 9), id, 3));   // v=3 → +9 last
        store.events.add(wrap(new Incremented(id, 3), id, 0));   // v=0 → +3 first
        store.events.add(wrap(new Reset(id), id, 2));    // v=2 → reset
        store.events.add(wrap(new Incremented(id, 4), id, 1));   // v=1 → +4

        Counter c = store.loadAggregate(id, Counter.class).orElseThrow();
        // After sorting by aggregateVersion: +3, +4, reset, +9 → 9
        assertEquals(9, c.value);
    }

    @Test
    void eventsAreAppliedInOrder() {
        UUID id = UUIDGenerator.newUuid();
        store.events.add(wrap(new Incremented(id, 3), id, 0));
        store.events.add(wrap(new Incremented(id, 4), id, 1));
        store.events.add(wrap(new Reset(id), id, 2));
        store.events.add(wrap(new Incremented(id, 9), id, 3));

        Counter c = store.loadAggregate(id, Counter.class).orElseThrow();
        assertEquals(9, c.value, "Reset clears, then +9");
    }

    @Test
    void contextIsPassedToHandlersThatDeclareIt() {
        UUID id = UUIDGenerator.newUuid();
        store.events.add(wrap(new Reset(id), id, 7));

        Counter c = store.loadAggregate(id, Counter.class).orElseThrow();
        assertEquals(1, c.seenContexts.size());
        assertEquals(7L, c.seenContexts.get(0).getAggregateVersion());
        assertEquals(id, c.seenContexts.get(0).getAggregateId());
    }

    @Test
    void unknownEventTypeIsSilentlySkipped() {
        UUID id = UUIDGenerator.newUuid();
        store.events.add(wrap(new Incremented(id, 2), id, 0));
        store.events.add(wrap(new Untracked(id), id, 1)); // not handled by Counter
        store.events.add(wrap(new Incremented(id, 3), id, 2));

        Counter c = store.loadAggregate(id, Counter.class).orElseThrow();
        assertEquals(5, c.value);
    }

    @Test
    void loadAggregateThrowsWhenTypeHasNoNoArgConstructor() {
        // Note: NoNoArgCtor isn't annotated as @Aggregate; we're only testing
        // the constructor path.
        assertThrows(MissingAggregateConstructorException.class,
                () -> store.loadAggregate(UUIDGenerator.newUuid(), NoNoArgCtor.class));
    }

    @Test
    void loadAggregateQueriesLoadEventsWithTheGivenAggregateId() {
        UUID id = UUIDGenerator.newUuid();
        store.loadAggregate(id, Counter.class);
        assertEquals(id, store.lastAggregateId);
    }

    private AggregateSnapshot snapshot(UUID id, int version, Object payload) {
        AggregateSnapshot s = new AggregateSnapshot();
        s.setAggregateId(id);
        s.setAggregateVersion(version);
        s.setSnapshot(serializer.serialize(payload));
        return s;
    }

    @Test
    void loadAggregateWithStoredSnapshotButNoSetterThrows() {
        // Counter has @EventHandler methods but no setSnapshot(...) — once a snapshot
        // exists for it, BaseEventStore cannot rehydrate it and must fail loudly
        // rather than silently start from a zero state on top of post-snapshot events.
        UUID id = UUIDGenerator.newUuid();
        store.snapshot = snapshot(id, 5, new CounterSnapshot(42));

        MissingAggregateSnapshotHandlerException ex = assertThrows(
                MissingAggregateSnapshotHandlerException.class,
                () -> store.loadAggregate(id, Counter.class));
        assertTrue(ex.getMessage().contains(Counter.class.getName()));
        assertTrue(ex.getMessage().contains("setSnapshot"));
    }

    @Test
    void loadAggregateAppliesSnapshotThenReplaysOnlyPostSnapshotEvents() {
        GlobalRegistry.register(SnapshotableCounter.class);
        UUID id = UUIDGenerator.newUuid();
        store.snapshot = snapshot(id, 4, new CounterSnapshot(10));
        // One post-snapshot event should be applied on top of the rehydrated state.
        store.events.add(wrap(new Incremented(id, 3), id, 5));

        SnapshotableCounter c = store.loadAggregate(id, SnapshotableCounter.class).orElseThrow();
        assertEquals(13, c.value, "snapshot value (10) + post-snapshot increment (3)");
        assertEquals(4L, store.lastFromVersion,
                "loadEvents must be called with snapshot.aggregateVersion as fromVersion");
    }

    @Test
    void loadAggregateWithThrowingSetterIsWrappedAsMissingHandler() {
        // setSnapshot throws → BaseEventStore wraps it as MissingAggregateSnapshotHandler.
        GlobalRegistry.register(BrokenSetterCounter.class);
        UUID id = UUIDGenerator.newUuid();
        store.snapshot = snapshot(id, 2, new CounterSnapshot(7));

        MissingAggregateSnapshotHandlerException ex = assertThrows(
                MissingAggregateSnapshotHandlerException.class,
                () -> store.loadAggregate(id, BrokenSetterCounter.class));
        assertTrue(ex.getMessage().contains("Invalid setter"));
        assertTrue(ex.getMessage().contains(BrokenSetterCounter.class.getName()));
    }

    @Test
    void aggregateVersionField_emptyStream_isZero() {
        GlobalRegistry.register(VersionedCounter.class);
        VersionedCounter c = store.loadAggregate(UUIDGenerator.newUuid(), VersionedCounter.class).orElseThrow();
        assertEquals(0L, c.version, "no snapshot + no events → version field initialised to 0");
    }

    @Test
    void aggregateVersionField_isBumpedToLastReplayedEventVersion() {
        GlobalRegistry.register(VersionedCounter.class);
        UUID id = UUIDGenerator.newUuid();
        store.events.add(wrap(new Incremented(id, 1), id, 0));
        store.events.add(wrap(new Incremented(id, 2), id, 1));
        store.events.add(wrap(new Incremented(id, 4), id, 2));

        VersionedCounter c = store.loadAggregate(id, VersionedCounter.class).orElseThrow();
        assertEquals(7, c.value);
        assertEquals(2L, c.version,
                "@AggregateVersion field must track the last applied event's aggregateVersion");
    }

    @Test
    void aggregateVersionField_seedsFromSnapshotThenTracksTailReplay() {
        GlobalRegistry.register(VersionedCounter.class);
        UUID id = UUIDGenerator.newUuid();
        store.snapshot = snapshot(id, 4, new CounterSnapshot(10));
        store.events.add(wrap(new Incremented(id, 3), id, 5));

        VersionedCounter c = store.loadAggregate(id, VersionedCounter.class).orElseThrow();
        assertEquals(13, c.value, "snapshot (10) + tail increment (3)");
        assertEquals(5L, c.version,
                "version field starts at snapshot.aggregateVersion and is overwritten by each replayed event");
    }

    @Test
    void aggregateVersionField_snapshotOnly_endsAtSnapshotVersion() {
        // Snapshot present, no post-snapshot events: version field must remain at the
        // value seeded from the snapshot.
        GlobalRegistry.register(VersionedCounter.class);
        UUID id = UUIDGenerator.newUuid();
        store.snapshot = snapshot(id, 9, new CounterSnapshot(42));

        VersionedCounter c = store.loadAggregate(id, VersionedCounter.class).orElseThrow();
        assertEquals(42, c.value);
        assertEquals(9L, c.version);
    }

    @Test
    void aggregateVersionField_intType_isAlsoWritten() {
        // writeVersion has a dedicated int/Integer branch; cover it.
        GlobalRegistry.register(IntVersionedCounter.class);
        UUID id = UUIDGenerator.newUuid();
        store.events.add(wrap(new Incremented(id, 5), id, 0));
        store.events.add(wrap(new Incremented(id, 6), id, 1));

        IntVersionedCounter c = store.loadAggregate(id, IntVersionedCounter.class).orElseThrow();
        assertEquals(11, c.value);
        assertEquals(1, c.version);
    }

    @Test
    void aggregateWithoutAggregateVersionField_loadStillSucceeds() {
        // Counter has no @AggregateVersion field — findVersionField returns null and
        // the load path must skip the writeVersion calls without NPEing.
        UUID id = UUIDGenerator.newUuid();
        store.events.add(wrap(new Incremented(id, 4), id, 0));
        Counter c = store.loadAggregate(id, Counter.class).orElseThrow();
        assertEquals(4, c.value);
    }

    @Test
    void aggregateWithoutRegisteredEventHandlersReturnsBareInstanceEvenWithEvents() {
        GlobalRegistry.register(NoHandlerCounter.class);
        UUID id = UUIDGenerator.newUuid();
        store.events.add(wrap(new Incremented(id, 5), id, 0));

        NoHandlerCounter c = store.loadAggregate(id, NoHandlerCounter.class).orElseThrow();
        assertEquals(0, c.value);
    }

    @Event
    public static class Incremented {
        @AggregateIdentifier
        public UUID id;
        public int amount;

        public Incremented() {
        }

        public Incremented(UUID id, int amount) {
            this.id = id;
            this.amount = amount;
        }
    }

    // ── snapshot restore ──────────────────────────────────────────────────────

    @Event
    public static class Reset {
        @AggregateIdentifier
        public UUID id;

        public Reset() {
        }

        public Reset(UUID id) {
            this.id = id;
        }
    }

    @Event
    public static class Untracked {
        @AggregateIdentifier
        public UUID id;

        public Untracked() {
        }

        public Untracked(UUID id) {
            this.id = id;
        }
    }

    @Aggregate
    public static class Counter {
        public final List<Context> seenContexts = new ArrayList<>();
        public int value;

        @EventHandler
        public void on(Incremented e) {
            value += e.amount;
        }

        @EventHandler
        public void on(Reset e, Context ctx) {
            value = 0;
            seenContexts.add(ctx);
        }
    }

    public static class NoNoArgCtor {
        public NoNoArgCtor(String required) {
        }
    }

    // ── @AggregateVersion field ───────────────────────────────────────────────

    @Aggregate
    public static class NoHandlerCounter {
        public int value;
    }

    /**
     * Test store that returns a fixture event list and tracks what was queried.
     */
    public static class FixtureStore extends BaseEventStore {
        public List<InternalMessage> events = new ArrayList<>();
        public UUID lastAggregateId;
        public AggregateSnapshot snapshot;
        public Long lastFromVersion;

        @Override
        public void appendEvents(List<InternalMessage> events) {
        }

        @Override
        public List<InternalMessage> loadEvents(UUID aggregateId, long fromVersion) {
            this.lastAggregateId = aggregateId;
            this.lastFromVersion = fromVersion;
            return events;
        }

        @Override
        public Optional<AggregateSnapshot> loadSnapshot(UUID aggregateId) {
            return Optional.ofNullable(snapshot);
        }

        @Override
        public void storeSnapshot(UUID aggregateId, Object snapshotPayload, long schemaVersion, long aggregateVersion) {
        }
    }

    /**
     * Snapshot payload type for the snapshot-restore tests.
     */
    public static class CounterSnapshot {
        public int value;

        public CounterSnapshot() {
        }

        public CounterSnapshot(int value) {
            this.value = value;
        }
    }

    /**
     * Aggregate that knows how to rehydrate from a {@link CounterSnapshot}.
     */
    @Aggregate
    public static class SnapshotableCounter {
        public int value;

        @EventHandler
        public void on(Incremented e) {
            value += e.amount;
        }

        public void setSnapshot(CounterSnapshot snap) {
            this.value = snap.value;
        }
    }

    /**
     * Aggregate with a @AggregateVersion-annotated long field — exercises the version write path.
     */
    @Aggregate
    public static class VersionedCounter {
        public int value;
        @AggregateVersion
        public long version;

        @EventHandler
        public void on(Incremented e) {
            value += e.amount;
        }

        public void setSnapshot(CounterSnapshot snap) {
            this.value = snap.value;
        }
    }

    /**
     * Same idea but with a primitive int version field — checks the int branch of writeVersion.
     */
    @Aggregate
    public static class IntVersionedCounter {
        public int value;
        @AggregateVersion
        public int version;

        @EventHandler
        public void on(Incremented e) {
            value += e.amount;
        }
    }

    /**
     * Aggregate whose setSnapshot deliberately blows up — exercises the wrap-in-exception path.
     */
    @Aggregate
    public static class BrokenSetterCounter {
        public int value;

        @EventHandler
        public void on(Incremented e) {
            value += e.amount;
        }

        public void setSnapshot(CounterSnapshot snap) {
            throw new RuntimeException("boom");
        }
    }
}
