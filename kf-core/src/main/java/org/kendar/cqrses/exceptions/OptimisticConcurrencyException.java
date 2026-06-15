package org.kendar.cqrses.exceptions;

/**
 * Thrown by an {@code EventStore} append when the per-aggregate version a command
 * computed is no longer free: another writer appended to the same aggregate first
 * (the cluster's two-nodes-one-aggregate race, caught by the
 * {@code UNIQUE(aggregate_id, sequence)} backstop) or an explicit
 * {@code send(command, expectedVersion)} no longer matches the stream head.
 *
 * <p>Extends {@link IllegalStateException} for backward compatibility — callers and
 * tests that matched the historical {@code IllegalStateException("Optimistic
 * concurrency violation …")} still match. The <b>synchronous</b> command path catches
 * this type specifically to retry with a fresh aggregate load (which sees the winner's
 * event and recomputes the next version) before giving up and propagating to the sender.
 */
public class OptimisticConcurrencyException extends IllegalStateException {

    public OptimisticConcurrencyException(String message) {
        super(message);
    }

    public OptimisticConcurrencyException(String message, Throwable cause) {
        super(message, cause);
    }
}
