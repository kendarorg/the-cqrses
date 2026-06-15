package org.kendar.cqrses.bus;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Per-message envelope (aggregate id/version, type, processing group, trace id,
 * metadata, timestamp). Persisted (e.g. by a durable DLQ store capturing the
 * processing context of a failed message) through the registered
 * {@code MessageSerializer} like any other payload — the JSR-310 module is
 * registered on {@code JacksonMessageSerializer}, so the {@link Instant}
 * timestamp round-trips as JSON. See {@code docs/tricks.md}.
 */
public class Context {

    private UUID aggregateId;
    private long aggregateVersion = -1; // -1 = "assign on append"; EventStore replaces with currentMax+1

    private String type;
    private long version;
    private String processingGroup;
    private UUID traceId;
    private Map<String, String> metadata = new LinkedHashMap<>();
    private Instant timestamp = Instant.now();

    public String getProcessingGroup() {
        return processingGroup;
    }

    public void setProcessingGroup(String processingGroup) {
        this.processingGroup = processingGroup;
    }

    public UUID getTraceId() {
        return traceId;
    }

    public void setTraceId(UUID traceId) {
        this.traceId = traceId;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public UUID getAggregateId() {
        return aggregateId;
    }

    public void setAggregateId(UUID aggregateId) {
        this.aggregateId = aggregateId;
    }

    public long getAggregateVersion() {
        return aggregateVersion;
    }

    public void setAggregateVersion(long aggregateVersion) {
        this.aggregateVersion = aggregateVersion;
    }
}
