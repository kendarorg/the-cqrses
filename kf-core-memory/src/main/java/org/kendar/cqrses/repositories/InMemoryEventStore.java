package org.kendar.cqrses.repositories;

import org.kendar.cqrses.bus.Context;
import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.exceptions.OptimisticConcurrencyException;
import org.kendar.cqrses.pg.SegmentCalculator;
import org.kendar.cqrses.serialization.MessageSerializer;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Process-local {@link EventStore} backed by ConcurrentHashMaps, keyed by
 * {@code segment} (a pure function of the aggregate id) and nested per-aggregate.
 * This mirrors JDBC's {@code event_entry}: {@code processing_group} is a recorded
 * field on each event but never a read key — {@code loadEvents}/{@code loadSnapshot}
 * resolve by {@code aggregate_id} alone. Because the segment is derivable from the
 * id, reads need no ambient state (no thread-local group). Append uses a
 * per-aggregate lock so optimistic concurrency checks and version assignment
 * cannot race. Snapshot payload + version are stored as a single record so a
 * concurrent {@code loadAggregate} cannot observe a torn pair.
 */
public class InMemoryEventStore extends BaseEventStore {

    private final ConcurrentHashMap<Integer, ConcurrentHashMap<UUID, CopyOnWriteArrayList<InternalMessage>>> eventStreams = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<UUID, SnapshotEntry>> snapshots = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<UUID, Object>> streamLocks = new ConcurrentHashMap<>();

    // Per-segment, append-ordered log + gap-free counter for the pollable tail.
    // The unified per-segment stream (across groups/aggregates) the in-memory
    // mirror of event_entry's (segment, segment_seq) — see JdbcEventStore.
    private final ConcurrentHashMap<Integer, CopyOnWriteArrayList<InternalMessage>> segmentLog = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, AtomicLong> segmentSeq = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Object> segmentLocks = new ConcurrentHashMap<>();

    @Override
    public void appendEvents(List<InternalMessage> events) {
        if (events == null || events.isEmpty()) return;

        ConcurrentHashMap<UUID, List<InternalMessage>> byAggregate = new ConcurrentHashMap<>();
        for (InternalMessage e : events) {
            UUID aggregateId = e.getContext().getAggregateId();
            if (aggregateId == null) {
                throw new IllegalArgumentException("InternalMessage.context.aggregateId must not be null");
            }
            byAggregate.computeIfAbsent(aggregateId, k -> new ArrayList<>()).add(e);
        }

        for (var entry : byAggregate.entrySet()) {
            UUID aggregateId = entry.getKey();
            List<InternalMessage> aggregateEvents = entry.getValue();
            // All of one aggregate's events share a segment — a pure function of
            // the aggregate id. processing_group is recorded on each event but is
            // not a storage key, exactly as in JDBC's event_entry.
            int segment = SegmentCalculator.calculateSegment(aggregateId);

            Object lock = streamLocks.computeIfAbsent(segment, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(aggregateId, k -> new Object());

            synchronized (lock) {
                CopyOnWriteArrayList<InternalMessage> stream =
                        eventStreams.computeIfAbsent(segment, k -> new ConcurrentHashMap<>())
                                .computeIfAbsent(aggregateId, k -> new CopyOnWriteArrayList<>());

                long currentMax = stream.stream()
                        .mapToLong(m -> m.getContext().getAggregateVersion())
                        .max()
                        .orElse(-1L);

                for (InternalMessage msg : aggregateEvents) {
                    long version = msg.getContext().getAggregateVersion();
                    if (version == -1L) {
                        // -1 = "assign next on append". Replace with currentMax+1 so
                        // stored events always carry a concrete monotonic version,
                        // otherwise snapshot tail replay would silently skip them.
                        currentMax++;
                        msg.getContext().setAggregateVersion(currentMax);
                    } else {
                        if (version != currentMax + 1) {
                            throw new OptimisticConcurrencyException(
                                    "Optimistic concurrency violation for aggregate " + aggregateId +
                                            ": expected version " + (currentMax + 1) + " but got " + version);
                        }
                        currentMax = version;
                    }
                    stream.add(msg);
                }

                // Assign gap-free segment_seq + createdAt and index into the
                // per-segment tail log. Under a per-segment lock (acquired while
                // holding the per-aggregate lock — segment lock is always the leaf,
                // so no deadlock) so concurrent appends to the same segment by
                // different aggregates stay strictly ordered and gap-free.
                Object segLock = segmentLocks.computeIfAbsent(segment, k -> new Object());
                synchronized (segLock) {
                    AtomicLong counter = segmentSeq.computeIfAbsent(segment, k -> new AtomicLong(0));
                    CopyOnWriteArrayList<InternalMessage> log =
                            segmentLog.computeIfAbsent(segment, k -> new CopyOnWriteArrayList<>());
                    long now = Instant.now().toEpochMilli();
                    for (InternalMessage msg : aggregateEvents) {
                        long seq = counter.getAndIncrement();
                        msg.setSegmentSeq(seq);
                        msg.setCreatedAt(now);
                        log.add(msg);
                    }
                }
            }
        }
    }

    @Override
    public List<InternalMessage> loadSegmentTail(int segment, long afterSeq, int limit) {
        CopyOnWriteArrayList<InternalMessage> log = segmentLog.get(segment);
        if (log == null) return Collections.emptyList();
        List<InternalMessage> out = new ArrayList<>();
        for (InternalMessage m : log) {
            if (m.getSegmentSeq() > afterSeq) {
                out.add(copyForTail(m));
                if (out.size() >= limit) break;
            }
        }
        return out;
    }

    @Override
    public List<InternalMessage> loadSegmentsTail(Map<Integer, Long> afterSeqBySegment, int limit) {
        if (afterSeqBySegment == null || afterSeqBySegment.isEmpty()) return Collections.emptyList();
        List<InternalMessage> out = new ArrayList<>();
        for (Map.Entry<Integer, Long> e : afterSeqBySegment.entrySet()) {
            CopyOnWriteArrayList<InternalMessage> log = segmentLog.get(e.getKey());
            if (log == null) continue;
            long after = e.getValue();
            for (InternalMessage m : log) {
                if (m.getSegmentSeq() > after) {
                    out.add(copyForTail(m));
                }
            }
        }
        // Mirror the JDBC ORDER BY segment, segment_seq + LIMIT so both stores'
        // multi-segment tail reads behave identically (exact within a segment).
        out.sort(Comparator.<InternalMessage>comparingInt(
                        m -> SegmentCalculator.calculateSegment(m.getContext().getAggregateId()))
                .thenComparingLong(InternalMessage::getSegmentSeq));
        if (out.size() > limit) {
            return new ArrayList<>(out.subList(0, limit));
        }
        return out;
    }

    @Override
    public List<InternalMessage> loadSegmentTypeTail(int sourceSegment, Set<String> eventTypes,
                                                     long afterSeq, int limit) {
        if (eventTypes == null || eventTypes.isEmpty()) return Collections.emptyList();
        CopyOnWriteArrayList<InternalMessage> log = segmentLog.get(sourceSegment);
        if (log == null) return Collections.emptyList();
        List<InternalMessage> out = new ArrayList<>();
        for (InternalMessage m : log) {
            if (m.getSegmentSeq() > afterSeq && eventTypes.contains(m.getContext().getType())) {
                out.add(copyForTail(m));
                if (out.size() >= limit) break;
            }
        }
        return out;
    }

    // Fresh InternalMessage with the same context fields the JDBC tailMapper
    // exposes, so both stores' tail reads behave identically (no aliasing of the
    // stored stream reference into the dispatcher).
    private static InternalMessage copyForTail(InternalMessage src) {
        Context sc = src.getContext();
        Context c = new Context();
        c.setAggregateId(sc.getAggregateId());
        c.setAggregateVersion(sc.getAggregateVersion());
        c.setType(sc.getType());
        c.setProcessingGroup(sc.getProcessingGroup());
        InternalMessage m = new InternalMessage();
        m.setEvent(true);
        m.setContext(c);
        m.setPayload(src.getPayload());
        m.setSegmentSeq(src.getSegmentSeq());
        m.setCreatedAt(src.getCreatedAt());
        return m;
    }

    @Override
    public List<InternalMessage> loadEvents(UUID aggregateId, long fromVersion) {
        int segment = SegmentCalculator.calculateSegment(aggregateId);
        ConcurrentHashMap<UUID, CopyOnWriteArrayList<InternalMessage>> partition = eventStreams.get(segment);
        if (partition == null) return Collections.emptyList();
        CopyOnWriteArrayList<InternalMessage> stream = partition.get(aggregateId);
        if (stream == null) return Collections.emptyList();
        if (fromVersion == -1L) return new ArrayList<>(stream);
        return stream.stream()
                .filter(e -> e.getContext().getAggregateVersion() > fromVersion)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<AggregateSnapshot> loadSnapshot(UUID aggregateId) {
        int segment = SegmentCalculator.calculateSegment(aggregateId);
        ConcurrentHashMap<UUID, SnapshotEntry> partition = snapshots.get(segment);
        if (partition == null) return Optional.empty();
        SnapshotEntry entry = partition.get(aggregateId);
        if (entry == null) return Optional.empty();
        AggregateSnapshot snap = new AggregateSnapshot();
        snap.setAggregateId(aggregateId);
        snap.setAggregateVersion(entry.version());
        snap.setSnapshot(entry.payload());
        snap.setSchemaVersion(entry.schemaVersion());
        snap.setSnapshotType(entry.snapshotType());
        return Optional.of(snap);
    }

    @Override
    public void storeSnapshot(UUID aggregateId, Object snapshotPayload, long schemaVersion, long aggregateVersion) {
        if (snapshotPayload == null) {
            throw new IllegalStateException(
                    "Snapshot payload for aggregate " + aggregateId + " must not be null");
        }
        int segment = SegmentCalculator.calculateSegment(aggregateId);
        // aggregateVersion == -1: stamp the current event-stream max — the highest
        // event already folded into the aggregate state being snapshotted. Read it
        // under the same per-aggregate lock so a racing append cannot bump the
        // stream between version read and snapshot write. An explicit version (the
        // automatic trigger's path) is trusted as-is.
        Object lock = streamLocks.computeIfAbsent(segment, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(aggregateId, k -> new Object());
        synchronized (lock) {
            int version;
            if (aggregateVersion >= 0) {
                version = (int) aggregateVersion;
            } else {
                ConcurrentHashMap<UUID, CopyOnWriteArrayList<InternalMessage>> partition = eventStreams.get(segment);
                CopyOnWriteArrayList<InternalMessage> stream = partition == null ? null : partition.get(aggregateId);
                version = stream == null ? -1 : (int) stream.stream()
                        .mapToLong(m -> m.getContext().getAggregateVersion())
                        .max()
                        .orElse(-1L);
            }
            byte[] payload = GlobalRegistry.get(MessageSerializer.class).serialize(snapshotPayload);
            snapshots.computeIfAbsent(segment, k -> new ConcurrentHashMap<>())
                    .put(aggregateId, new SnapshotEntry(payload, version, schemaVersion,
                            snapshotPayload.getClass().getSimpleName()));
        }
    }

    private record SnapshotEntry(byte[] payload, int version, long schemaVersion, String snapshotType) {
    }
}
