package org.kendar.cqrses.repositories;

import org.kendar.cqrses.db.Db;

import java.time.Instant;

/**
 * JDBC {@link CheckpointStore} over the {@code processor_checkpoint} table. All
 * DML in the H2 &cap; MySQL intersection ({@code ON DUPLICATE KEY UPDATE}).
 */
public class JdbcCheckpointStore implements CheckpointStore {

    private final Db db;

    public JdbcCheckpointStore(Db db) {
        this.db = db;
    }

    @Override
    public long load(String processingGroup, int segment, int sourceSegment) {
        Long v = db.queryForObject(
                "SELECT last_seq FROM processor_checkpoint " +
                        "WHERE processing_group = ? AND segment = ? AND source_segment = ?",
                Long.class, processingGroup, segment, sourceSegment);
        return v == null ? -1L : v;
    }

    @Override
    public void save(String processingGroup, int segment, int sourceSegment, long lastSeq) {
        // Monotonic via GREATEST (H2 ∩ MySQL): a stale lower write during owner-overlap
        // cannot regress the cursor. updated_at still bumps so liveness is observable.
        db.update(
                "INSERT INTO processor_checkpoint(processing_group, segment, source_segment, last_seq, updated_at) " +
                        "VALUES (?,?,?,?,?) " +
                        "ON DUPLICATE KEY UPDATE last_seq = GREATEST(last_seq, VALUES(last_seq)), " +
                        "updated_at = VALUES(updated_at)",
                processingGroup, segment, sourceSegment, lastSeq, Instant.now().toEpochMilli());
    }

    @Override
    public void reset(String processingGroup, int segment, int sourceSegment, long lastSeq) {
        // Force-write, including backward — for operator replay only.
        db.update(
                "INSERT INTO processor_checkpoint(processing_group, segment, source_segment, last_seq, updated_at) " +
                        "VALUES (?,?,?,?,?) " +
                        "ON DUPLICATE KEY UPDATE last_seq = VALUES(last_seq), updated_at = VALUES(updated_at)",
                processingGroup, segment, sourceSegment, lastSeq, Instant.now().toEpochMilli());
    }
}
