package org.kendar.cqrses.dlq;


import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Persistence layer for Dead Letter Queue items.
 * <p>
 * Register an instance in GlobalRegistry so the buses can discover it:
 * <pre>
 *     GlobalRegistry.register(DlqStore.class, new InMemoryDlqStore());
 * </pre>
 */
public interface DlqStore {
    boolean hasBlockedItems(String sequenceId);

    void evictFirst(String sequenceId);
    void addItem(DlqItem item, String sequenceId);


    /**
     * Return all PENDING items for the given consumer class name.
     */
    List<DlqItem> listItems(String sequenceId);

    /**
     * Return any item by id regardless of status.
     */
    Optional<DlqItem> getItem(UUID id);

    /**
     * Update only the status field of an existing item.
     */
    void updateStatus(UUID id, DlqItemStatus status);

    /**
     * Replace all mutable fields of an existing item (used for retry-count updates).
     * Must preserve the item's position in its sequence queue — a retry that bumps
     * the retry count and leaves the item PENDING must not reorder the FIFO.
     */
    void updateItem(DlqItem item);

    /**
     * Remove the item with {@code id} from both its per-sequence queue and the
     * by-id index, wherever it sits in the queue (not just the head). Used by a
     * DlqManager to resolve/dismiss/redispatch a specific dead letter and clear
     * the head-of-line block it was holding. No-op if the id is unknown.
     */
    void removeItem(UUID id);

    void clear();

}
