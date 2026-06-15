package org.kendar.cqrses.dlq;


import org.kendar.cqrses.bus.Context;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Represents a single failed event that has been routed to the Dead Letter Queue.
 * <p>
 * The {@code serializedEvent} field carries the JSON-serialized form of the original event,
 * used by JDBC-backed retry. The {@code originalEvent} field is a transient in-memory
 * reference used by the in-memory DlqManager to avoid a serialization round-trip on retry.
 */
public class DlqItem {
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DlqItem dlqItem)) return false;
        return Objects.equals(getId(), dlqItem.getId()) && Objects.equals(getSequenceId(), dlqItem.getSequenceId()) && Objects.equals(getProcessingGroup(), dlqItem.getProcessingGroup());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getSequenceId(), getProcessingGroup());
    }

    private UUID id;
    private String sequenceId;
    private String processingGroup;

    /**
     * Serialized (JSON) event payload — populated when an EventSerializer is available.
     */
    private byte[] serializedEvent;

    /**
     * Fully-qualified class name of the domain event.
     */
    private String eventType;

    /**
     * Aggregate ID extracted from the event; may be null for events without an aggregate.
     */
    private UUID aggregateId;

    /**
     * Processing context captured at the moment of failure. Restored on retry so
     * handlers and interceptors see the same correlation/trace IDs and metadata
     * they would have seen on the original dispatch.
     */
    private Context processingContext;

    private String errorMessage;
    private String errorClass;
    private String stackTrace;
    private Instant failedAt;
    private int retryCount;
    private DlqItemStatus status;

    /**
     * Error info from the last retry attempt, if any. Distinct from
     * {@code errorMessage}/{@code errorClass}/{@code stackTrace} (the ORIGINAL
     * failure) so neither view overwrites the other.
     */
    private String lastRetryErrorMessage;
    private String lastRetryErrorClass;
    private String lastRetryStackTrace;
    private Instant lastRetryAt;

    /**
     * Direct reference to the original event object.
     * Populated by LocalEventBus for zero-copy retry; transient so persistent stores skip it.
     */
    private transient Object originalEvent;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getSequenceId() {
        return sequenceId;
    }

    public void setSequenceId(String sequenceId) {
        this.sequenceId = sequenceId;
    }


    public byte[] getSerializedEvent() {
        return serializedEvent;
    }

    public void setSerializedEvent(byte[] serializedEvent) {
        this.serializedEvent = serializedEvent;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public UUID getAggregateId() {
        return aggregateId;
    }

    public void setAggregateId(UUID aggregateId) {
        this.aggregateId = aggregateId;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getErrorClass() {
        return errorClass;
    }

    public void setErrorClass(String errorClass) {
        this.errorClass = errorClass;
    }

    public String getStackTrace() {
        return stackTrace;
    }

    public void setStackTrace(String stackTrace) {
        this.stackTrace = stackTrace;
    }

    public Instant getFailedAt() {
        return failedAt;
    }

    public void setFailedAt(Instant failedAt) {
        this.failedAt = failedAt;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public DlqItemStatus getStatus() {
        return status;
    }

    public void setStatus(DlqItemStatus status) {
        this.status = status;
    }

    public Object getOriginalEvent() {
        return originalEvent;
    }

    public void setOriginalEvent(Object originalEvent) {
        this.originalEvent = originalEvent;
    }

    public Context getProcessingContext() {
        return processingContext;
    }

    public void setProcessingContext(Context processingContext) {
        this.processingContext = processingContext;
    }

    public String getLastRetryErrorMessage() {
        return lastRetryErrorMessage;
    }

    public void setLastRetryErrorMessage(String s) {
        this.lastRetryErrorMessage = s;
    }

    public String getLastRetryErrorClass() {
        return lastRetryErrorClass;
    }

    public void setLastRetryErrorClass(String s) {
        this.lastRetryErrorClass = s;
    }

    public String getLastRetryStackTrace() {
        return lastRetryStackTrace;
    }

    public void setLastRetryStackTrace(String s) {
        this.lastRetryStackTrace = s;
    }

    public Instant getLastRetryAt() {
        return lastRetryAt;
    }

    public void setLastRetryAt(Instant t) {
        this.lastRetryAt = t;
    }

    public String getProcessingGroup() {
        return processingGroup;
    }

    public void setProcessingGroup(String processingGroup) {
        this.processingGroup = processingGroup;
    }
}

