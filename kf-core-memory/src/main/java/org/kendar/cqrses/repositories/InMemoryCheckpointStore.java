package org.kendar.cqrses.repositories;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Process-local {@link CheckpointStore} backed by a ConcurrentHashMap. For tests,
 * single-node demos and the contract suites — not for cluster deployments (those
 * wire {@link JdbcCheckpointStore} so a new owner resumes another node's cursor).
 */
public class InMemoryCheckpointStore implements CheckpointStore {

    private record Key(String group, int segment, int sourceSegment) {
    }

    private final ConcurrentHashMap<Key, Long> checkpoints = new ConcurrentHashMap<>();

    @Override
    public long load(String processingGroup, int segment, int sourceSegment) {
        return checkpoints.getOrDefault(new Key(processingGroup, segment, sourceSegment), -1L);
    }

    @Override
    public void save(String processingGroup, int segment, int sourceSegment, long lastSeq) {
        // Monotonic: a stale lower write during owner-overlap can never regress the cursor.
        checkpoints.merge(new Key(processingGroup, segment, sourceSegment), lastSeq, Math::max);
    }

    @Override
    public void reset(String processingGroup, int segment, int sourceSegment, long lastSeq) {
        // Force-write, including backward — for operator replay only.
        checkpoints.put(new Key(processingGroup, segment, sourceSegment), lastSeq);
    }
}
