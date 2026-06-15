package org.kendar.cqrses.exceptions;

import java.util.UUID;

/**
 * Thrown when a DLQ retry re-invokes the handler and it fails again. The original
 * {@link Throwable} is not recoverable (the DLQ persists only its string form), so
 * this carries the recorded class name and message of the re-failure. The retry
 * attempt is already persisted on the item ({@code retryCount} bumped, status back
 * to {@code PENDING}, {@code lastRetry*} fields set) before this is thrown.
 */
public class DlqRetryFailedException extends RuntimeException {
    private final UUID itemId;
    private final String failedErrorClass;

    public DlqRetryFailedException(UUID itemId, String failedErrorClass, String failedErrorMessage) {
        super("Retry of DLQ item " + itemId + " failed again: ["
                + failedErrorClass + "] " + failedErrorMessage);
        this.itemId = itemId;
        this.failedErrorClass = failedErrorClass;
    }

    public UUID getItemId() {
        return itemId;
    }

    /** Fully-qualified class name of the exception the re-invocation threw. */
    public String getFailedErrorClass() {
        return failedErrorClass;
    }
}
