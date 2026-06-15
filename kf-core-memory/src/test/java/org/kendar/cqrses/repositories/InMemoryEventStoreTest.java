package org.kendar.cqrses.repositories;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.bus.Context;
import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.pg.SegmentCalculator;
import org.kendar.cqrses.serialization.JacksonMessageSerializer;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.utils.UUIDGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryEventStoreTest {

    private InMemoryEventStore store;
    private MessageSerializer<?,?> serializer;

    private static InternalMessage msg(UUID aggregateId, long version, String type) {
        InternalMessage m = new InternalMessage();
        Context c = new Context();
        c.setAggregateId(aggregateId);
        c.setAggregateVersion(version);
        c.setType(type);
        m.setContext(c);
        m.setPayload(new byte[]{1, 2, 3});
        return m;
    }

    @BeforeEach
    void setUp() {
        GlobalRegistry.clear();
        serializer = new JacksonMessageSerializer();
        GlobalRegistry.register(MessageSerializer.class, serializer);
        store = new InMemoryEventStore();
    }

    @AfterEach
    void tearDown() {
        GlobalRegistry.clear();
    }

    @Test
    void appendAndLoadInOrder() {
        UUID agg = UUIDGenerator.newUuid();
        store.appendEvents(List.of(msg(agg, 0, "A"), msg(agg, 1, "B")));

        List<InternalMessage> loaded = store.loadEvents(agg);
        assertEquals(2, loaded.size());
        assertEquals("A", loaded.get(0).getContext().getType());
        assertEquals("B", loaded.get(1).getContext().getType());
    }

    // ── append / load basics ─────────────────────────────────────────────────

    @Test
    void loadEventsRespectsFromVersion() {
        UUID agg = UUIDGenerator.newUuid();
        store.appendEvents(List.of(msg(agg, 0, "A"), msg(agg, 1, "B"), msg(agg, 2, "C")));

        // fromVersion is strict greater-than → fromVersion=1 returns versions > 1.
        List<InternalMessage> tail = store.loadEvents(agg, 1);
        assertEquals(1, tail.size());
        assertEquals("C", tail.get(0).getContext().getType());
    }

    @Test
    void loadUnknownAggregateReturnsEmptyList() {
        assertTrue(store.loadEvents(UUIDGenerator.newUuid()).isEmpty());
        assertTrue(store.loadEvents(UUIDGenerator.newUuid(), 0).isEmpty());
    }

    @Test
    void loadEventsFromVersionPastEndReturnsEmpty() {
        UUID agg = UUIDGenerator.newUuid();
        store.appendEvents(List.of(msg(agg, 0, "A"), msg(agg, 1, "B")));
        assertTrue(store.loadEvents(agg, 99).isEmpty());
    }

    @Test
    void loadEventsFromVersionZero_skipsVersionZero() {
        UUID agg = UUIDGenerator.newUuid();
        store.appendEvents(List.of(msg(agg, 0, "A"), msg(agg, 1, "B")));
        // fromVersion uses strict >, so 0 filters out version 0.
        List<InternalMessage> tail = store.loadEvents(agg, 0);
        assertEquals(1, tail.size());
        assertEquals("B", tail.get(0).getContext().getType());
    }

    @Test
    void loadEventsWithFromVersionMinusOne_returnsEntireStream() {
        UUID agg = UUIDGenerator.newUuid();
        store.appendEvents(List.of(msg(agg, 0, "A"), msg(agg, 1, "B"), msg(agg, 2, "C")));
        assertEquals(3, store.loadEvents(agg, -1L).size());
    }

    @Test
    void appendNullOrEmptyBatchIsNoOp() {
        UUID agg = UUIDGenerator.newUuid();
        assertDoesNotThrow(() -> store.appendEvents(null));
        assertDoesNotThrow(() -> store.appendEvents(List.of()));
        assertTrue(store.loadEvents(agg).isEmpty());
    }

    @Test
    void appendBatchWithSequentialVersionsInOneCall() {
        UUID agg = UUIDGenerator.newUuid();
        store.appendEvents(List.of(msg(agg, 0, "A"), msg(agg, 1, "B"), msg(agg, 2, "C")));
        List<InternalMessage> all = store.loadEvents(agg);
        assertEquals(3, all.size());
        for (int i = 0; i < 3; i++) {
            assertEquals(i, all.get(i).getContext().getAggregateVersion());
        }
    }

    // ── versioning / OCC ─────────────────────────────────────────────────────

    @Test
    void appendingOutOfOrderVersionThrows() {
        UUID agg = UUIDGenerator.newUuid();
        store.appendEvents(List.of(msg(agg, 0, "A")));
        assertThrows(IllegalStateException.class,
                () -> store.appendEvents(List.of(msg(agg, 2, "C"))));
    }

    @Test
    void appendBatchWithGapInsideSingleCallThrows() {
        UUID agg = UUIDGenerator.newUuid();
        assertThrows(IllegalStateException.class,
                () -> store.appendEvents(List.of(msg(agg, 0, "A"), msg(agg, 2, "C"))));
    }

    @Test
    void appendDuplicateVersionThrows() {
        UUID agg = UUIDGenerator.newUuid();
        store.appendEvents(List.of(msg(agg, 0, "A")));
        assertThrows(IllegalStateException.class,
                () -> store.appendEvents(List.of(msg(agg, 0, "A2"))));
    }

    @Test
    void appendWithVersionMinusOne_assignsNextVersion() {
        UUID agg = UUIDGenerator.newUuid();
        store.appendEvents(List.of(msg(agg, 0, "A")));
        store.appendEvents(List.of(msg(agg, -1L, "B")));

        List<InternalMessage> loaded = store.loadEvents(agg);
        assertEquals(2, loaded.size());
        assertEquals(0, loaded.get(0).getContext().getAggregateVersion());
        assertEquals(1, loaded.get(1).getContext().getAggregateVersion(),
                "-1 sentinel must be replaced with currentMax+1, not stored as -1");
    }

    @Test
    void appendBatchMixingMinusOneAndExplicit_resolvesInOrder() {
        UUID agg = UUIDGenerator.newUuid();
        store.appendEvents(List.of(msg(agg, 0, "A")));
        store.appendEvents(List.of(msg(agg, -1L, "B"), msg(agg, 2, "C")));
        List<InternalMessage> all = store.loadEvents(agg);
        assertEquals(3, all.size());
        assertEquals(0, all.get(0).getContext().getAggregateVersion());
        assertEquals(1, all.get(1).getContext().getAggregateVersion());
        assertEquals(2, all.get(2).getContext().getAggregateVersion());
    }

    @Test
    void appendOnlyMinusOneEvents_assignsContiguousVersionsFromZero() {
        UUID agg = UUIDGenerator.newUuid();
        store.appendEvents(List.of(
                msg(agg, -1L, "A"), msg(agg, -1L, "B"), msg(agg, -1L, "C")));
        List<InternalMessage> all = store.loadEvents(agg);
        assertEquals(3, all.size());
        assertEquals(0, all.get(0).getContext().getAggregateVersion());
        assertEquals(1, all.get(1).getContext().getAggregateVersion());
        assertEquals(2, all.get(2).getContext().getAggregateVersion());
    }

    @Test
    void appendEventWithNullAggregateIdThrows() {
        InternalMessage bad = new InternalMessage();
        Context c = new Context();
        c.setAggregateId(null);
        c.setAggregateVersion(0);
        c.setType("X");
        bad.setContext(c);
        bad.setPayload(new byte[]{});
        assertThrows(IllegalArgumentException.class,
                () -> store.appendEvents(List.of(bad)));
    }

    @Test
    void appendsToDifferentAggregatesShareNoVersionSpace() {
        UUID a = UUIDGenerator.newUuid();
        UUID b = UUIDGenerator.newUuid();
        store.appendEvents(List.of(msg(a, 0, "A1"), msg(a, 1, "A2"), msg(a, 2, "A3")));
        store.appendEvents(List.of(msg(b, 0, "B1")));
        assertEquals(3, store.loadEvents(a).size());
        assertEquals(1, store.loadEvents(b).size());
    }

    @Test
    void concurrentAppendOnSameAggregate_exactlyOneWinsEachVersion() throws Exception {
        UUID agg = UUIDGenerator.newUuid();
        store.appendEvents(List.of(msg(agg, 0, "seed")));

        int threads = 8;
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger conflicts = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    ready.countDown();
                    try {
                        go.await();
                        store.appendEvents(List.of(msg(agg, 1, "race")));
                        successes.incrementAndGet();
                    } catch (IllegalStateException e) {
                        conflicts.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            ready.await();
            go.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }

        assertEquals(1, successes.get(), "exactly one thread should win version 1");
        assertEquals(threads - 1, conflicts.get(), "all other threads must observe a conflict");
        assertEquals(2, store.loadEvents(agg).size(),
                "only one event for version 1 must be persisted");
    }

    @Test
    void appendBatchGroupedByAggregate_independentVersions() {
        UUID a = UUIDGenerator.newUuid();
        UUID b = UUIDGenerator.newUuid();
        List<InternalMessage> mixed = new ArrayList<>();
        mixed.add(msg(a, 0, "A1"));
        mixed.add(msg(b, 0, "B1"));
        mixed.add(msg(a, 1, "A2"));
        mixed.add(msg(b, 1, "B2"));
        store.appendEvents(mixed);

        assertEquals(2, store.loadEvents(a).size());
        assertEquals(2, store.loadEvents(b).size());
    }

    @Test
    void snapshotRoundTrip() {
        UUID agg = UUIDGenerator.newUuid();
        store.storeSnapshot(agg, new SnapPayload("hello"));

        Optional<AggregateSnapshot> snap = store.loadSnapshot(agg);
        assertTrue(snap.isPresent());
        assertEquals(agg, snap.get().getAggregateId());
        SnapPayload payload = serializer.deserialize(snap.get().getSnapshot(), SnapPayload.class);
        assertEquals("hello", payload.value);
    }

    // ── snapshots ────────────────────────────────────────────────────────────

    @Test
    void loadSnapshotForUnknownAggregateIsEmpty() {
        assertTrue(store.loadSnapshot(UUIDGenerator.newUuid()).isEmpty());
    }

    @Test
    void storeSnapshotOverwritesPreviousPayload() {
        UUID agg = UUIDGenerator.newUuid();
        store.storeSnapshot(agg, new SnapPayload("first"));
        store.storeSnapshot(agg, new SnapPayload("second"));

        SnapPayload reloaded = serializer.deserialize(
                store.loadSnapshot(agg).get().getSnapshot(), SnapPayload.class);
        assertEquals("second", reloaded.value);
    }

    @Test
    void storeSnapshotWithNullAggregateThrows() {
        UUID agg = UUIDGenerator.newUuid();
        assertThrows(IllegalStateException.class, () -> store.storeSnapshot(agg, null));
        assertTrue(store.loadSnapshot(agg).isEmpty(),
                "failed snapshot must not leave partial state behind");
    }

    @Test
    void snapshotIsIsolatedPerAggregate() {
        UUID a = UUIDGenerator.newUuid();
        UUID b = UUIDGenerator.newUuid();
        store.storeSnapshot(a, new SnapPayload("a-value"));
        store.storeSnapshot(b, new SnapPayload("b-value"));

        SnapPayload pa = serializer.deserialize(store.loadSnapshot(a).get().getSnapshot(), SnapPayload.class);
        SnapPayload pb = serializer.deserialize(store.loadSnapshot(b).get().getSnapshot(), SnapPayload.class);
        assertEquals("a-value", pa.value);
        assertEquals("b-value", pb.value);
    }

    @Test
    void snapshotDoesNotAffectEventStream() {
        UUID agg = UUIDGenerator.newUuid();
        store.appendEvents(List.of(msg(agg, 0, "A"), msg(agg, 1, "B")));
        store.storeSnapshot(agg, new SnapPayload("s"));

        List<InternalMessage> all = store.loadEvents(agg);
        assertEquals(2, all.size(), "storing a snapshot must not drop events");
        assertEquals("A", all.get(0).getContext().getType());
        assertEquals("B", all.get(1).getContext().getType());
    }

    @Test
    void snapshotVersion_matchesCurrentStreamMaxAtStoreTime() {
        UUID agg = UUIDGenerator.newUuid();
        store.appendEvents(List.of(msg(agg, 0, "A"), msg(agg, 1, "B"), msg(agg, 2, "C")));
        store.storeSnapshot(agg, new SnapPayload("s"));

        assertEquals(2, store.loadSnapshot(agg).get().getAggregateVersion());
    }

    @Test
    void snapshotVersion_isMinusOneWhenNoEventsExist() {
        UUID agg = UUIDGenerator.newUuid();
        store.storeSnapshot(agg, new SnapPayload("s"));
        assertEquals(-1, store.loadSnapshot(agg).get().getAggregateVersion());
    }

    @Test
    void appendingAfterSnapshotKeepsVersionMonotonic() {
        UUID agg = UUIDGenerator.newUuid();
        store.appendEvents(List.of(msg(agg, 0, "A"), msg(agg, 1, "B"), msg(agg, 2, "C")));
        store.storeSnapshot(agg, new SnapPayload("s3"));

        assertThrows(IllegalStateException.class,
                () -> store.appendEvents(List.of(msg(agg, 2, "C2"))),
                "snapshot must not reset the OCC counter");

        store.appendEvents(List.of(msg(agg, 3, "D")));
        assertEquals(4, store.loadEvents(agg).size());
    }

    @Test
    void snapshotTailReplay_loadEventsHonorsCursor() {
        UUID agg = UUIDGenerator.newUuid();
        store.appendEvents(List.of(
                msg(agg, 0, "A"), msg(agg, 1, "B"), msg(agg, 2, "C"), msg(agg, 3, "D")));
        store.storeSnapshot(agg, new SnapPayload("s"));

        int snapshotVersion = store.loadSnapshot(agg).get().getAggregateVersion();
        // snapshotVersion == 3 (current max), tail strictly past it is empty.
        assertTrue(store.loadEvents(agg, snapshotVersion).isEmpty());

        // Partial replay from cursor=1 → returns events with version > 1.
        List<InternalMessage> partial = store.loadEvents(agg, 1);
        assertEquals(2, partial.size());
        assertEquals("C", partial.get(0).getContext().getType());
        assertEquals("D", partial.get(1).getContext().getType());
    }

    // ---- segment tail reads (the durable pollable tail for the cluster pump) ----

    private static UUID aggInSegment(int segment) {
        while (true) {
            UUID u = UUIDGenerator.newUuid();
            if (SegmentCalculator.calculateSegment(u) == segment) return u;
        }
    }

    @Test
    void segmentTail_ordersGapFreeWithCreatedAt() {
        UUID agg = UUIDGenerator.newUuid();
        int seg = SegmentCalculator.calculateSegment(agg);
        store.appendEvents(List.of(msg(agg, 0, "A"), msg(agg, 1, "B"), msg(agg, 2, "C")));
        List<InternalMessage> tail = store.loadSegmentTail(seg, -1L, 100);
        assertEquals(3, tail.size());
        for (int i = 0; i < tail.size(); i++) {
            assertEquals(i, tail.get(i).getSegmentSeq(), "segment_seq must be gap-free from 0");
            assertTrue(tail.get(i).getCreatedAt() > 0, "createdAt must be populated on tail reads");
        }
        assertEquals("A", tail.get(0).getContext().getType());
        assertEquals("C", tail.get(2).getContext().getType());
    }

    @Test
    void segmentSeq_isGapFreeAcrossAggregatesInSameSegment() {
        int seg = 0;
        UUID a = aggInSegment(seg);
        UUID b = aggInSegment(seg);
        store.appendEvents(List.of(msg(a, 0, "A1"), msg(a, 1, "A2")));
        store.appendEvents(List.of(msg(b, 0, "B1")));
        List<InternalMessage> tail = store.loadSegmentTail(seg, -1L, 100);
        assertEquals(3, tail.size());
        assertEquals(0, tail.get(0).getSegmentSeq());
        assertEquals(1, tail.get(1).getSegmentSeq());
        assertEquals(2, tail.get(2).getSegmentSeq());
    }

    @Test
    void segmentTail_afterSeqIsACursor() {
        UUID agg = UUIDGenerator.newUuid();
        int seg = SegmentCalculator.calculateSegment(agg);
        store.appendEvents(List.of(msg(agg, 0, "A"), msg(agg, 1, "B"), msg(agg, 2, "C")));
        List<InternalMessage> tail = store.loadSegmentTail(seg, 0L, 100);
        assertEquals(2, tail.size());
        assertEquals("B", tail.get(0).getContext().getType());
        assertEquals("C", tail.get(1).getContext().getType());
    }

    @Test
    void segmentTail_limitCaps() {
        UUID agg = UUIDGenerator.newUuid();
        int seg = SegmentCalculator.calculateSegment(agg);
        store.appendEvents(List.of(msg(agg, 0, "A"), msg(agg, 1, "B"), msg(agg, 2, "C")));
        assertEquals(2, store.loadSegmentTail(seg, -1L, 2).size());
    }

    @Test
    void segmentTail_unknownSegmentEmpty() {
        assertTrue(store.loadSegmentTail(99, -1L, 100).isEmpty());
    }

    @Test
    void segmentTypeTail_filtersByType() {
        UUID agg = UUIDGenerator.newUuid();
        int seg = SegmentCalculator.calculateSegment(agg);
        store.appendEvents(List.of(msg(agg, 0, "A"), msg(agg, 1, "B"), msg(agg, 2, "A")));
        List<InternalMessage> onlyA = store.loadSegmentTypeTail(seg, Set.of("A"), -1L, 100);
        assertEquals(2, onlyA.size());
        assertEquals("A", onlyA.get(0).getContext().getType());
        assertEquals("A", onlyA.get(1).getContext().getType());
        assertEquals(0, onlyA.get(0).getSegmentSeq());
        assertEquals(2, onlyA.get(1).getSegmentSeq());
    }

    @Test
    void segmentTypeTail_emptyTypeSetReturnsEmpty() {
        UUID agg = UUIDGenerator.newUuid();
        int seg = SegmentCalculator.calculateSegment(agg);
        store.appendEvents(List.of(msg(agg, 0, "A")));
        assertTrue(store.loadSegmentTypeTail(seg, Set.of(), -1L, 100).isEmpty());
    }

    // ---- wide multi-segment tail read (the one-thread-per-group pump) ----
    // Same contract as JdbcEventStoreTest so both backends behave identically.

    @Test
    void segmentsTail_mergesOwnedSegmentsOrderedBySegmentThenSeq() {
        UUID a0 = aggInSegment(0);
        UUID a1 = aggInSegment(1);
        store.appendEvents(List.of(msg(a0, 0, "A0a"), msg(a0, 1, "A0b")));
        store.appendEvents(List.of(msg(a1, 0, "A1a")));
        List<InternalMessage> tail = store.loadSegmentsTail(Map.of(0, -1L, 1, -1L), 100);
        assertEquals(List.of("A0a", "A0b", "A1a"),
                tail.stream().map(m -> m.getContext().getType()).toList());
    }

    @Test
    void segmentsTail_eachSegmentHonorsItsOwnCursor() {
        UUID a0 = aggInSegment(0);
        UUID a1 = aggInSegment(1);
        store.appendEvents(List.of(msg(a0, 0, "A0a"), msg(a0, 1, "A0b")));
        store.appendEvents(List.of(msg(a1, 0, "A1a"), msg(a1, 1, "A1b")));
        List<InternalMessage> tail = store.loadSegmentsTail(Map.of(0, 0L, 1, -1L), 100);
        assertEquals(List.of("A0b", "A1a", "A1b"),
                tail.stream().map(m -> m.getContext().getType()).toList());
    }

    @Test
    void segmentsTail_limitCapsAcrossSegments() {
        UUID a0 = aggInSegment(0);
        UUID a1 = aggInSegment(1);
        store.appendEvents(List.of(msg(a0, 0, "A0a"), msg(a0, 1, "A0b")));
        store.appendEvents(List.of(msg(a1, 0, "A1a")));
        assertEquals(2, store.loadSegmentsTail(Map.of(0, -1L, 1, -1L), 2).size());
    }

    @Test
    void segmentsTail_emptyMapReturnsEmpty() {
        UUID a0 = aggInSegment(0);
        store.appendEvents(List.of(msg(a0, 0, "A0a")));
        assertTrue(store.loadSegmentsTail(Map.of(), 100).isEmpty());
    }

    @Test
    void segmentsTail_unownedSegmentsAreNotReturned() {
        UUID a0 = aggInSegment(0);
        UUID a1 = aggInSegment(1);
        store.appendEvents(List.of(msg(a0, 0, "A0a")));
        store.appendEvents(List.of(msg(a1, 0, "A1a")));
        List<InternalMessage> tail = store.loadSegmentsTail(Map.of(0, -1L), 100);
        assertEquals(List.of("A0a"), tail.stream().map(m -> m.getContext().getType()).toList());
    }

    public static class SnapPayload {
        public String value;

        public SnapPayload() {
        }

        public SnapPayload(String value) {
            this.value = value;
        }
    }
}
