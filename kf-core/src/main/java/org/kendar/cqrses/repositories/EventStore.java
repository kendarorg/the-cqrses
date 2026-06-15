package org.kendar.cqrses.repositories;

import org.kendar.cqrses.bus.InternalMessage;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public interface EventStore {
    void appendEvents(List<InternalMessage> events);

    List<InternalMessage> loadEvents(UUID aggregateId, long fromVersion);

    default List<InternalMessage> loadEvents(UUID aggregateId) {
        return loadEvents(aggregateId, -1L);
    }

    /**
     * Projection tail read: all events in {@code segment} with
     * {@code segment_seq > afterSeq}, in exact {@code segment_seq} order, up to
     * {@code limit}. Returned messages expose {@code segmentSeq} and
     * {@code createdAt}. The default throws — every durable/pollable store must
     * override (see {@code docs/tricks.md}).
     */
    default List<InternalMessage> loadSegmentTail(int segment, long afterSeq, int limit) {
        throw new UnsupportedOperationException(
                getClass().getName() + " does not support segment tail reads");
    }

    /**
     * Wide projection tail read across several owned segments in one round-trip:
     * every event whose {@code segment} appears in {@code afterSeqBySegment} and
     * whose {@code segment_seq} is strictly greater than that segment's mapped
     * cursor, ordered by {@code (segment, segment_seq)} — exact within each segment
     * (what projections need), cross-segment order unspecified (not needed) — up to
     * a total of {@code limit} rows. Because {@code segment_seq} is per-segment and
     * gap-free, each segment carries its own cursor; there is no single global
     * cursor. Lets one per-group worker poll all the segments a node owns with one
     * statement instead of one read per owned segment. The default throws — every
     * durable/pollable store must override (see {@code docs/tricks.md}).
     */
    default List<InternalMessage> loadSegmentsTail(Map<Integer, Long> afterSeqBySegment, int limit) {
        throw new UnsupportedOperationException(
                getClass().getName() + " does not support multi-segment tail reads");
    }

    /**
     * Saga tail read: events in one SOURCE {@code segment}, filtered to
     * {@code eventTypes}, with {@code segment_seq > afterSeq}, in exact
     * {@code segment_seq} order, up to {@code limit}. The caller merges across
     * source segments by {@code createdAt}. The default throws.
     */
    default List<InternalMessage> loadSegmentTypeTail(int sourceSegment, Set<String> eventTypes,
                                                      long afterSeq, int limit) {
        throw new UnsupportedOperationException(
                getClass().getName() + " does not support segment type tail reads");
    }

    Optional<AggregateSnapshot> loadSnapshot(UUID aggregateId);

    /**
     * Store {@code snapshotPayload} for {@code aggregateId}, stamped with the
     * schema revision resolved from {@code @Aggregate.version()} on the payload's
     * class when present (the aggregate doubling as its own snapshot), else
     * {@code 1}, and with the aggregate version stamped from the stream's current
     * max. Callers snapshotting through a separate DTO on a revisioned aggregate
     * should use the explicit overload instead.
     */
    default void storeSnapshot(UUID aggregateId, Object snapshotPayload) {
        long schemaVersion = 1;
        if (snapshotPayload != null) {
            var ann = snapshotPayload.getClass()
                    .getAnnotation(org.kendar.cqrses.annotations.Aggregate.class);
            if (ann != null) schemaVersion = ann.version();
        }
        storeSnapshot(aggregateId, snapshotPayload, schemaVersion, -1L);
    }

    /**
     * Store {@code snapshotPayload} for {@code aggregateId} stamped with an
     * explicit snapshot schema revision ({@code @Aggregate.version()}) and an
     * explicit aggregate version. {@code aggregateVersion == -1} means "stamp the
     * stream's current max" — best-effort: a command committing between the
     * caller's fold and this write makes the stamp newer than the payload state.
     * The automatic trigger always passes the exact version of the batch it just
     * appended, which has no such window.
     */
    void storeSnapshot(UUID aggregateId, Object snapshotPayload, long schemaVersion, long aggregateVersion);

    <T> Optional<T> loadAggregate(UUID aggregateId, Class<T> aggregateType);
}
