package org.kendar.cqrses.dlq;


public enum DlqItemStatus {
    /**
     * Waiting for operator action (retry or dismiss).
     */
    PENDING,

    /**
     * A retry has been initiated and is in flight.
     */
    RETRYING,

    /**
     * The item was successfully re-processed after a retry.
     */
    RESOLVED,

    /**
     * The item was explicitly dismissed without retrying.
     */
    DISMISSED
}

