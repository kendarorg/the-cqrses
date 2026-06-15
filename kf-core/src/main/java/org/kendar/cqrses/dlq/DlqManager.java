package org.kendar.cqrses.dlq;

import java.util.UUID;

/**
 * Operator-facing controls for resolving dead letters. Each operation can target
 * a single {@link DlqItem} by its {@link UUID} or an entire {@code sequenceId}
 * (the head-of-line-blocked stream), processed in FIFO order.
 *
 * <ul>
 *   <li>{@code retry}    — synchronously re-invoke the failed handler in an
 *       isolated, throwaway processing group. On success the item is resolved and
 *       its head-of-line block cleared; on re-failure the attempt is recorded
 *       ({@code retryCount}, {@code lastRetry*}) and a
 *       {@link org.kendar.cqrses.exceptions.DlqRetryFailedException} is thrown.</li>
 *   <li>{@code dismiss}  — give up on the item: mark it {@code DISMISSED} and
 *       clear its block. No re-invocation.</li>
 *   <li>{@code redispatch} — clear the block and re-enqueue the message onto the
 *       live worker so it flows through the normal pipeline again (asynchronous,
 *       fire-and-forget; a re-failure dead-letters a fresh item).</li>
 * </ul>
 *
 * <p>v1 is event-side only — see {@link LocalDlqManager}.
 */
public interface DlqManager {
    void retry(UUID itemId);

    void retry(String sequenceId);

    void dismiss(UUID itemId);

    void dismiss(String sequenceId);

    void redispatch(UUID itemId);

    void redispatch(String sequenceId);
}
