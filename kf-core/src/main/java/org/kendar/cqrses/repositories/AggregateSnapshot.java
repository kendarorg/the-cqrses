package org.kendar.cqrses.repositories;

import java.util.UUID;

public class AggregateSnapshot {
    private int aggregateVersion;
    private UUID aggregateId;
    private byte[] snapshot;
    // Snapshot schema revision (@Aggregate.version at write time). Rows written
    // before schema versioning existed read back as 1 — the annotation default.
    private long schemaVersion = 1;
    // Simple class name of the serialized snapshot payload, the upcaster origin
    // key. Null on legacy rows; the loader falls back to the setter's param type.
    private String snapshotType;

    public long getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(long schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public String getSnapshotType() {
        return snapshotType;
    }

    public void setSnapshotType(String snapshotType) {
        this.snapshotType = snapshotType;
    }

    public int getAggregateVersion() {
        return aggregateVersion;
    }

    public void setAggregateVersion(int aggregateVersion) {
        this.aggregateVersion = aggregateVersion;
    }

    public UUID getAggregateId() {
        return aggregateId;
    }

    public void setAggregateId(UUID aggregateId) {
        this.aggregateId = aggregateId;
    }

    public byte[] getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(byte[] snapshot) {
        this.snapshot = snapshot;
    }
}
