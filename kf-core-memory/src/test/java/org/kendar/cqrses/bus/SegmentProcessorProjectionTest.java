package org.kendar.cqrses.bus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.di.TargetType;
import org.kendar.cqrses.observability.Observability;
import org.kendar.cqrses.observability.ObservabilityInterface;
import org.kendar.cqrses.pg.SegmentCalculator;
import org.kendar.cqrses.pg.SegmentProcessor;
import org.kendar.cqrses.repositories.CheckpointStore;
import org.kendar.cqrses.repositories.InMemoryCheckpointStore;
import org.kendar.cqrses.repositories.InMemoryDlqStore;
import org.kendar.cqrses.repositories.InMemoryEventStore;
import org.kendar.cqrses.serialization.JacksonMessageSerializer;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.utils.UUIDGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Part 2: the projection pull path. With the event manager in pull mode the bus
 * spawns no lane threads; events reach the projection only via the
 * {@link SegmentProcessor} polling the {@link InMemoryEventStore} tail and
 * advancing a {@link CheckpointStore} per owned {@code (group, segment)}.
 */
class SegmentProcessorProjectionTest {

    // The test helper bus.registerHandler(...) routes every handler to the default
    // processing group regardless of the name passed, so the pull workers run under
    // "default" — that's the group the checkpoints are keyed by.
    private static final String GROUP = "default";

    private MessageSerializer<?, ?> serializer;
    private TestableInMemoryEventBus bus;
    private InMemoryEventStore eventStore;
    private CheckpointStore checkpointStore;
    private SegmentProcessor segProc;
    private final List<Thread> claimers = new ArrayList<>();

    @BeforeEach
    void setUp() {
        GlobalRegistry.clear();
        serializer = new JacksonMessageSerializer();
        GlobalRegistry.register(MessageSerializer.class, serializer);
        bus = new TestableInMemoryEventBus(serializer, new TestFixtures.StubSagaStore(), new InMemoryDlqStore());
        GlobalRegistry.register(EventBus.class, bus);
        eventStore = new InMemoryEventStore();
        GlobalRegistry.register(org.kendar.cqrses.repositories.EventStore.class, eventStore);
        checkpointStore = new InMemoryCheckpointStore();
    }

    @AfterEach
    void tearDown() {
        if (segProc != null) segProc.stopAll();
        for (Thread t : claimers) {
            try {
                t.join(1000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            bus.stop();
        } catch (Exception ignored) {
        }
        GlobalRegistry.clear();
    }

    private InternalMessage stored(UUID agg, String type, Object event) {
        Context c = new Context();
        c.setAggregateId(agg);
        c.setAggregateVersion(-1L);
        c.setType(type);
        c.setProcessingGroup(GROUP);
        InternalMessage m = new InternalMessage();
        m.setEvent(true);
        m.setContext(c);
        m.setPayload(serializer.serialize(event));
        return m;
    }

    private void startClaimingAllSegments() {
        for (int seg = 0; seg < SegmentCalculator.getSegments(); seg++) {
            final int s = seg;
            Thread t = new Thread(() -> segProc.claimSegment(s), "claim-" + s);
            t.setDaemon(true);
            claimers.add(t);
            t.start();
        }
    }

    private static void awaitSize(CopyOnWriteArrayList<?> list, int expected) {
        long deadline = System.currentTimeMillis() + 5_000;
        while (list.size() < expected && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    void pullPump_dispatchesEachStoredEventExactlyOnce() {
        CopyOnWriteArrayList<UUID> seen = new CopyOnWriteArrayList<>();
        GlobalRegistry.register(TestFixtures.ThingProjection.class, new TestFixtures.ThingProjection());
        bus.registerHandler(TestFixtures.ThingProjection.class, TestFixtures.ThingCreated.class,
                TargetType.PROJECTION, GROUP,
                (t, m, ctx) -> seen.add(((TestFixtures.ThingCreated) m).id));
        bus.getHandler().setPullMode(true);
        bus.start();

        // Append events spread across segments (distinct aggregates).
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            UUID id = UUIDGenerator.newUuid();
            ids.add(id);
            eventStore.appendEvents(List.of(stored(id, "ThingCreated", new TestFixtures.ThingCreated(id, "v" + i))));
        }

        segProc = new SegmentProcessor(bus.getHandler(), eventStore, checkpointStore);
        startClaimingAllSegments();

        awaitSize(seen, ids.size());
        assertEquals(ids.size(), seen.size(), "every stored event must be dispatched exactly once");
        assertTrue(seen.containsAll(ids));
    }

    @Test
    void dispatchConcurrency_partitionedPoolStillDispatchesEachEventOnce() {
        // Two dispatch slots per group (segment % 2): each owned segment maps to a
        // single slot, so per-aggregate ordering holds and no event is dispatched twice.
        CopyOnWriteArrayList<UUID> seen = new CopyOnWriteArrayList<>();
        GlobalRegistry.register(TestFixtures.ThingProjection.class, new TestFixtures.ThingProjection());
        bus.registerHandler(TestFixtures.ThingProjection.class, TestFixtures.ThingCreated.class,
                TargetType.PROJECTION, GROUP,
                (t, m, ctx) -> seen.add(((TestFixtures.ThingCreated) m).id));
        bus.getHandler().setPullMode(true);
        bus.start();

        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            UUID id = UUIDGenerator.newUuid();
            ids.add(id);
            eventStore.appendEvents(List.of(stored(id, "ThingCreated", new TestFixtures.ThingCreated(id, "v" + i))));
        }

        segProc = new SegmentProcessor(bus.getHandler(), eventStore, checkpointStore,
                SegmentProcessor.DEFAULT_BATCH, 2);
        startClaimingAllSegments();

        awaitSize(seen, ids.size());
        assertEquals(ids.size(), seen.size(), "every event dispatched exactly once across slots");
        assertTrue(seen.containsAll(ids));
    }

    @Test
    void pullMode_localPushIsNoOp() throws Exception {
        CopyOnWriteArrayList<UUID> seen = new CopyOnWriteArrayList<>();
        GlobalRegistry.register(TestFixtures.ThingProjection.class, new TestFixtures.ThingProjection());
        bus.registerHandler(TestFixtures.ThingProjection.class, TestFixtures.ThingCreated.class,
                TargetType.PROJECTION, GROUP,
                (t, m, ctx) -> seen.add(((TestFixtures.ThingCreated) m).id));
        bus.getHandler().setPullMode(true);
        bus.start();

        // No SegmentProcessor running: a direct publish must NOT reach the lane.
        bus.send(new TestFixtures.ThingCreated(UUIDGenerator.newUuid(), "v"));
        Thread.sleep(200);
        assertTrue(seen.isEmpty(), "pull mode must make the local lane push a no-op");
    }

    @Test
    void checkpoint_advancesAndResumeIsANoOp() {
        CopyOnWriteArrayList<UUID> seen = new CopyOnWriteArrayList<>();
        GlobalRegistry.register(TestFixtures.ThingProjection.class, new TestFixtures.ThingProjection());
        bus.registerHandler(TestFixtures.ThingProjection.class, TestFixtures.ThingCreated.class,
                TargetType.PROJECTION, GROUP,
                (t, m, ctx) -> seen.add(((TestFixtures.ThingCreated) m).id));
        bus.getHandler().setPullMode(true);
        bus.start();

        UUID id = UUIDGenerator.newUuid();
        int seg = SegmentCalculator.calculateSegment(id);
        eventStore.appendEvents(List.of(stored(id, "ThingCreated", new TestFixtures.ThingCreated(id, "v"))));

        segProc = new SegmentProcessor(bus.getHandler(), eventStore, checkpointStore);
        startClaimingAllSegments();
        awaitSize(seen, 1);
        assertEquals(1, seen.size());

        // The checkpoint for the owning segment must advance past the event. The
        // save happens just after dispatch (checkpoint-after-process), so poll.
        long deadline = System.currentTimeMillis() + 2_000;
        while (checkpointStore.load(GROUP, seg, seg) < 0 && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        assertTrue(checkpointStore.load(GROUP, seg, seg) >= 0, "checkpoint must advance after dispatch");

        // No new events → no re-dispatch (the high-water-mark is honored).
        try {
            Thread.sleep(150);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        assertEquals(1, seen.size(), "a caught-up pump must not re-dispatch");
    }

    @Test
    void replay_reReadsHistory_idempotentModelStable() {
        // Idempotent read model (a Set of ids) + a raw dispatch counter so we can
        // tell "re-ran every event" (counter doubles) from "model rebuilt cleanly"
        // (set size unchanged).
        Set<UUID> model = ConcurrentHashMap.newKeySet();
        AtomicInteger dispatches = new AtomicInteger();
        GlobalRegistry.register(TestFixtures.ThingProjection.class, new TestFixtures.ThingProjection());
        bus.registerHandler(TestFixtures.ThingProjection.class, TestFixtures.ThingCreated.class,
                TargetType.PROJECTION, GROUP,
                (t, m, ctx) -> {
                    model.add(((TestFixtures.ThingCreated) m).id);
                    dispatches.incrementAndGet();
                });
        bus.getHandler().setPullMode(true);
        bus.start();

        int n = 5;
        for (int i = 0; i < n; i++) {
            UUID id = UUIDGenerator.newUuid();
            eventStore.appendEvents(List.of(stored(id, "ThingCreated", new TestFixtures.ThingCreated(id, "v" + i))));
        }

        segProc = new SegmentProcessor(bus.getHandler(), eventStore, checkpointStore);
        startClaimingAllSegments();
        awaitInt(dispatches, n);
        assertEquals(n, model.size());

        // Replay the projection group from the start → re-tail every event.
        segProc.replay(Set.of(GROUP), 0L, false);
        awaitInt(dispatches, 2 * n);
        assertEquals(2 * n, dispatches.get(), "replay must re-run every event from fromSeq=0");
        assertEquals(n, model.size(), "idempotent read model unchanged after replay");
    }

    private static void awaitInt(AtomicInteger value, int expected) {
        long deadline = System.currentTimeMillis() + 5_000;
        while (value.get() < expected && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Counts checkpoint saves per (group, segment); every other hook is a no-op. */
    private static final class RecordingObservability implements ObservabilityInterface {
        final ConcurrentHashMap<String, AtomicInteger> checkpointSaves = new ConcurrentHashMap<>();

        @Override
        public void onCheckpointSaved(String group, int segment) {
            checkpointSaves.computeIfAbsent(group + "#" + segment, k -> new AtomicInteger())
                    .incrementAndGet();
        }

        int saves(String group, int segment) {
            AtomicInteger v = checkpointSaves.get(group + "#" + segment);
            return v == null ? 0 : v.get();
        }

        @Override
        public void onCommandHandled(String group, String commandType, long nanos, boolean ok) {
        }

        @Override
        public void onAggregateRehydrated(String aggregateType, int eventsReplayed, long nanos) {
        }

        @Override
        public void onEventsAppended(int count, long nanos) {
        }

        @Override
        public void onEventDispatched(String group, int segment, String eventType, long nanos, boolean ok) {
        }

        @Override
        public void onSagaDispatched(String group, int segment, String eventType, long nanos, boolean ok) {
        }

        @Override
        public void onSegmentTailRead(String group, int eventsRead, long nanos) {
        }

        @Override
        public void onDlqEnqueued(String group, String eventType) {
        }

        @Override
        public void onSqlExecuted(String category, long nanos, boolean ok) {
        }
    }

    @Test
    void checkpoint_savedOncePerBatchNotPerEvent() {
        // All events target ONE aggregate → one segment → a single wide read
        // returns them as one batch. The batched checkpoint commit must produce
        // exactly one onCheckpointSaved for that segment, not one per event.
        RecordingObservability recording = new RecordingObservability();
        Observability.set(recording);
        try {
            CopyOnWriteArrayList<UUID> seen = new CopyOnWriteArrayList<>();
            GlobalRegistry.register(TestFixtures.ThingProjection.class, new TestFixtures.ThingProjection());
            bus.registerHandler(TestFixtures.ThingProjection.class, TestFixtures.ThingCreated.class,
                    TargetType.PROJECTION, GROUP,
                    (t, m, ctx) -> seen.add(((TestFixtures.ThingCreated) m).id));
            bus.getHandler().setPullMode(true);
            bus.start();

            UUID agg = UUIDGenerator.newUuid();
            int seg = SegmentCalculator.calculateSegment(agg);
            int n = 8;
            for (int i = 0; i < n; i++) {
                eventStore.appendEvents(List.of(stored(agg, "ThingCreated",
                        new TestFixtures.ThingCreated(agg, "v" + i))));
            }

            segProc = new SegmentProcessor(bus.getHandler(), eventStore, checkpointStore);
            startClaimingAllSegments();
            awaitSize(seen, n);
            assertEquals(n, seen.size());

            // Wait for the durable checkpoint to land at the last event's seq.
            long deadline = System.currentTimeMillis() + 2_000;
            while (recording.saves(GROUP, seg) == 0 && System.currentTimeMillis() < deadline) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
            assertEquals(1, recording.saves(GROUP, seg),
                    "one batch of " + n + " events must commit the checkpoint once, not per event");
            assertTrue(checkpointStore.load(GROUP, seg, seg) >= 0,
                    "the single batched save must still advance the durable checkpoint");
        } finally {
            Observability.set(null);
        }
    }

    @Test
    void nudge_publishAfterIdleIsDispatchedThroughThePullPump() {
        // The pump starts with nothing to do and parks; a later append + publish
        // exercises the PumpNudger wakeup path (ProcessingGroupsManager.send in
        // pull mode) and the event must still be observed exactly once.
        CopyOnWriteArrayList<UUID> seen = new CopyOnWriteArrayList<>();
        GlobalRegistry.register(TestFixtures.ThingProjection.class, new TestFixtures.ThingProjection());
        bus.registerHandler(TestFixtures.ThingProjection.class, TestFixtures.ThingCreated.class,
                TargetType.PROJECTION, GROUP,
                (t, m, ctx) -> seen.add(((TestFixtures.ThingCreated) m).id));
        bus.getHandler().setPullMode(true);
        bus.start();

        segProc = new SegmentProcessor(bus.getHandler(), eventStore, checkpointStore);
        startClaimingAllSegments();

        // Let the workers go idle (no events yet).
        try {
            Thread.sleep(250);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        assertTrue(seen.isEmpty());

        // Local append + publish: the publish is a lane no-op in pull mode but
        // fires the nudge; the parked worker drains the new event.
        UUID id = UUIDGenerator.newUuid();
        TestFixtures.ThingCreated event = new TestFixtures.ThingCreated(id, "v");
        eventStore.appendEvents(List.of(stored(id, "ThingCreated", event)));
        bus.send(event);

        awaitSize(seen, 1);
        assertEquals(1, seen.size(), "a post-idle local publish must reach the projection via the pump");
        assertEquals(id, seen.get(0));
    }
}
