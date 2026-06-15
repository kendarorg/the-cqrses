package org.kendar.cqrses.exceptions;

/**
 * A forwarded command was delivered to the owning node but no response arrived
 * within the wait budget. The outcome is <b>ambiguous</b> — the remote may have
 * executed the command — so the framework deliberately does NOT fall back to
 * local execution (that would risk applying the command twice; OCC does not
 * deduplicate semantically distinct appends). The caller must decide: query the
 * read side, retry with an idempotent command, or surface the error.
 */
public class ForwardTimeoutException extends RuntimeException {
    public ForwardTimeoutException(String message) {
        super(message);
    }

    public ForwardTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
