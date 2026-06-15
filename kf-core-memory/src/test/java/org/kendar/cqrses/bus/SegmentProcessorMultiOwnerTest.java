package org.kendar.cqrses.bus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.di.TargetType;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Models the cluster's "exactly one owner per segment" guarantee in one JVM
 * (a true two-node test is impossible because {@code GlobalRegistry} is
 * process-wide static): two {@link SegmentProcessor}s share one event store,
 * checkpoint store and event manager but claim <b>disjoint</b> segments. Every
 * stored event must then be dispatched <b>exactly once</b> across the two
 * "nodes", and a segment handoff (release on one, claim on the other) must resume
 * from the shared checkpoint without re-dispatching already-processed events.
 */
class SegmentProcessorMultiOwnerTest {

    private static final String GROUP = "default";

    private MessageSerializer<?, ?> serializer;
    private TestableInMemoryEventBus bus;
    private InMemoryEventStore eventStore;
    private CheckpointStore checkpointStore;
    private CopyOnWriteArrayList<UUID> dispatched;
    private final List<SegmentProcessor> processors = new ArrayList<>();
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

        dispatched = new CopyOnWriteArrayList<>();
        GlobalRegistry.register(TestFixtures.ThingProjection.class, new TestFixtures.ThingProjection());
        bus.registerHandler(TestFixtures.ThingProjection.class, TestFixtures.ThingCreated.class,
                TargetType.PROJECTION, GROUP,
                (t, m, ctx) -> dispatched.add(((TestFixtures.ThingCreated) m).id));
        bus.getHandler().setPullMode(true);
        bus.start();
    }

    @AfterEach
    void tearDown() {
        for (SegmentProcessor p : processors) p.stopAll();
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

    private SegmentProcessor newNode() {
        SegmentProcessor p = new SegmentProcessor(bus.getHandler(), eventStore, checkpointStore);
        processors.add(p);
        return p;
    }

    private void claim(SegmentProcessor node, int seg) {
        Thread t = new Thread(() -> node.claimSegment(seg), "claim-" + seg);
        t.setDaemon(true);
        claimers.add(t);
        t.start();
    }

    private void append(UUID id, int i) {
        Context c = new Context();
        c.setAggregateId(id);
        c.setAggregateVersion(-1L);
        c.setType("ThingCreated");
        c.setProcessingGroup(GROUP);
        InternalMessage m = new InternalMessage();
        m.setEvent(true);
        m.setContext(c);
        m.setPayload(serializer.serialize(new TestFixtures.ThingCreated(id, "v" + i)));
        eventStore.appendEvents(List.of(m));
    }

    private void awaitDispatched(int expected) {
        long deadline = System.currentTimeMillis() + 5_000;
        while (dispatched.size() < expected && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    void disjointOwners_dispatchEachEventExactlyOnce() {
        int segments = SegmentCalculator.getSegments(); // 3
        // node A owns segment 0; node B owns the rest.
        SegmentProcessor nodeA = newNode();
        SegmentProcessor nodeB = newNode();
        claim(nodeA, 0);
        for (int s = 1; s < segments; s++) claim(nodeB, s);

        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            UUID id = UUIDGenerator.newUuid();
            ids.add(id);
            append(id, i);
        }

        awaitDispatched(ids.size());
        assertEquals(ids.size(), dispatched.size(), "every event dispatched exactly once across both nodes");
        assertEquals(new HashSet<>(ids), new HashSet<>(dispatched), "no duplicates, no misses");
    }

    @Test
    void segmentHandoff_resumesFromCheckpointWithoutReprocessing() {
        // Put several events into segment 1 first; node B owns 1, node A owns 0.
        List<UUID> seg1ids = new ArrayList<>();
        int i = 0;
        while (seg1ids.size() < 5) {
            UUID id = UUIDGenerator.newUuid();
            if (SegmentCalculator.calculateSegment(id) == 1) {
                seg1ids.add(id);
                append(id, i++);
            }
        }

        SegmentProcessor nodeA = newNode();
        SegmentProcessor nodeB = newNode();
        claim(nodeA, 0);
        claim(nodeB, 1);
        awaitDispatched(seg1ids.size());
        assertEquals(seg1ids.size(), dispatched.size());

        // Hand segment 1 off from B to A: release on B (drains + checkpoints), then A claims it.
        nodeB.releaseSegment(1, () -> {
        });
        // Give the async drain a moment to commit the checkpoint and stop the worker.
        try {
            Thread.sleep(100);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        claim(nodeA, 1);

        // New events into segment 1 are picked up by A; the original 5 are NOT re-dispatched.
        List<UUID> more = new ArrayList<>();
        while (more.size() < 4) {
            UUID id = UUIDGenerator.newUuid();
            if (SegmentCalculator.calculateSegment(id) == 1) {
                more.add(id);
                append(id, i++);
            }
        }
        awaitDispatched(seg1ids.size() + more.size());

        Set<UUID> expected = new HashSet<>(seg1ids);
        expected.addAll(more);
        assertEquals(expected.size(), dispatched.size(),
                "handoff resumed from checkpoint — no re-dispatch of the pre-handoff events");
        assertEquals(expected, new HashSet<>(dispatched));
    }
}
