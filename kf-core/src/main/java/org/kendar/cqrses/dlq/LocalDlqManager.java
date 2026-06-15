package org.kendar.cqrses.dlq;

import org.kendar.cqrses.bus.Bus;
import org.kendar.cqrses.bus.CommandBus;
import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.exceptions.DlqRetryFailedException;
import org.kendar.cqrses.pg.ProcessingGroup;
import org.kendar.cqrses.pg.ProcessingGroupsManager;
import org.kendar.cqrses.serialization.UpcastersManager;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Node-local {@link DlqManager}. Resolves dead letters against the same frozen
 * bus topology that produced them — it only READS the bus's consumers/policy and
 * re-invokes; it never subscribes handlers, mutates the consumers map, or touches
 * {@code GlobalRegistry}, so it is safe to use at runtime (CLAUDE.md frozen-topology
 * rule).
 *
 * <p><b>Retry mechanism.</b> The live worker for a blocked sequence cannot retry
 * an item: its {@code hasBlockedItems} gate routes everything straight back to the
 * DLQ while the item sits in the store. So {@code retry} builds a <i>throwaway</i>
 * {@link ProcessingGroup} for the item's group, backed by a {@link CapturingDlqStore}
 * (whose {@code hasBlockedItems} is always false, so dispatch actually runs) and an
 * {@link AlwaysEnqueueOnError} policy (so a re-failure is captured instead of being
 * swallowed by the group's real IGNORE policy). The throwaway group is invoked
 * synchronously — never started as a thread — exactly like
 * {@link ProcessingGroupsManager#sendSync}. If nothing was captured, the handler
 * succeeded.
 *
 * <p><b>v1 is event-side only.</b> Command-side dispatch re-persists and republishes
 * a command's emitted events on success, so retrying a command would duplicate them.
 * Constructing this over a {@link CommandBus} throws.
 */
public class LocalDlqManager implements DlqManager {

    private final Bus bus;
    private final DlqStore store;
    private final ProcessingGroupsManager liveManager;
    private final UpcastersManager upcastersManager;

    /**
     * @param bus         the (event) bus whose frozen topology produced the dead letters
     * @param store       the real DLQ store holding the dead letters
     * @param liveManager the live processing-groups manager of {@code bus}, used by
     *                    {@code redispatch} to re-deliver onto the running worker
     *                    (push: enqueue on the lane; pull: synchronous live dispatch)
     */
    public LocalDlqManager(Bus bus, DlqStore store, ProcessingGroupsManager liveManager) {
        this.upcastersManager = GlobalRegistry.get(UpcastersManager.class);
        if (bus instanceof CommandBus) {
            throw new UnsupportedOperationException(
                    "command-side DLQ retry is not supported: re-running a command would "
                            + "duplicate its emitted events. Wire the DlqManager to the event bus.");
        }
        this.bus = bus;
        this.store = store;
        this.liveManager = liveManager;
    }

    // ── retry ──────────────────────────────────────────────────────────────────

    @Override
    public void retry(UUID itemId) {
        retryItem(require(itemId));
    }

    @Override
    public void retry(String sequenceId) {
        // Snapshot FIFO order; each success removes its item, each failure throws.
        for (DlqItem item : store.listItems(sequenceId)) {
            retryItem(item);
        }
    }

    private void retryItem(DlqItem item) {
        assertNotTerminal(item);
        if (item.getStatus() == DlqItemStatus.RETRYING) {
            throw new IllegalStateException("DLQ item " + item.getId() + " is already RETRYING");
        }
        store.updateStatus(item.getId(), DlqItemStatus.RETRYING);

        var capturing = new CapturingDlqStore();
        var pg = freshGroup(item.getProcessingGroup(), capturing);
        var itemMessage = rebuild(item);
        pg.invokeConsumers(itemMessage, consumersFor(item,itemMessage));

        DlqItem captured = capturing.captured;
        if (captured == null) {
            // Success: resolve and clear the head-of-line block.
            store.updateStatus(item.getId(), DlqItemStatus.RESOLVED);
            store.removeItem(item.getId());
            return;
        }
        // Re-failure: record the attempt, leave it PENDING and blocking, then throw.
        item.setRetryCount(item.getRetryCount() + 1);
        item.setStatus(DlqItemStatus.PENDING);
        item.setLastRetryAt(Instant.now());
        item.setLastRetryErrorMessage(captured.getErrorMessage());
        item.setLastRetryErrorClass(captured.getErrorClass());
        item.setLastRetryStackTrace(captured.getStackTrace());
        store.updateItem(item);
        throw new DlqRetryFailedException(item.getId(), captured.getErrorClass(), captured.getErrorMessage());
    }

    // ── dismiss ────────────────────────────────────────────────────────────────

    @Override
    public void dismiss(UUID itemId) {
        dismissItem(require(itemId));
    }

    @Override
    public void dismiss(String sequenceId) {
        for (DlqItem item : store.listItems(sequenceId)) {
            dismissItem(item);
        }
    }

    private void dismissItem(DlqItem item) {
        assertNotTerminal(item);
        store.updateStatus(item.getId(), DlqItemStatus.DISMISSED);
        store.removeItem(item.getId());
    }

    // ── redispatch ─────────────────────────────────────────────────────────────

    @Override
    public void redispatch(UUID itemId) {
        redispatchItem(require(itemId));
    }

    @Override
    public void redispatch(String sequenceId) {
        for (DlqItem item : store.listItems(sequenceId)) {
            redispatchItem(item);
        }
    }

    private void redispatchItem(DlqItem item) {
        assertNotTerminal(item);
        if (liveManager == null) {
            throw new IllegalStateException("redispatch requires a live ProcessingGroupsManager");
        }
        var msg = rebuild(item);
        // Clear the block first, then hand the message back to the live worker. A
        // re-failure dead-letters a fresh item through the normal pipeline. redeliver
        // (not send) so this works in pull mode too — where send() is a no-op and the
        // dispatch must go synchronously through the live group.
        store.removeItem(item.getId());
        liveManager.redeliver(Set.of(item.getProcessingGroup()), msg);
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    private DlqItem require(UUID itemId) {
        return store.getItem(itemId)
                .orElseThrow(() -> new IllegalArgumentException("DLQ item not found: " + itemId));
    }

    private static void assertNotTerminal(DlqItem item) {
        var status = item.getStatus();
        if (status == DlqItemStatus.RESOLVED || status == DlqItemStatus.DISMISSED) {
            throw new IllegalStateException(
                    "DLQ item " + item.getId() + " is " + status + " and cannot be acted on");
        }
    }

    private InternalMessage rebuild(DlqItem item) {
        if (item.getProcessingContext() == null) {
            throw new IllegalStateException(
                    "DLQ item " + item.getId() + " has no processingContext; it predates Context capture "
                            + "and cannot be retried/redispatched");
        }
        var msg = new InternalMessage();
        msg.setPayload(item.getSerializedEvent());
        msg.setContext(item.getProcessingContext());
        msg.setEvent(true);
        msg.setRetry(true);
        return upcastersManager.upcast(msg);
    }

    private List<Bus.Registration> consumersFor(DlqItem item, InternalMessage itemMessage) {
        var group = item.getProcessingGroup();
        var eventType = itemMessage.getContext().getType();
        var perGroup = bus.getConsumers(group);
        if (perGroup == null) {
            throw new IllegalStateException("No consumers registered for processing group '" + group + "'");
        }
        var messageType = bus.getMessageClass(eventType);
        if (messageType == null) {
            throw new IllegalStateException(
                    "Unknown event type '" + eventType + "' — no handler subscribes to it on this node");
        }
        var list = perGroup.get(messageType);
        if (list == null) {
            throw new IllegalStateException(
                    "No consumer for '" + eventType + "' in processing group '" + group + "'");
        }
        return list;
    }

    private ProcessingGroup freshGroup(String group, CapturingDlqStore capturing) {
        var basePolicy = bus.getProcessingGroupPolicy(group);
        var retryPolicy = new Bus.ProcessingGroupPolicyConfig(
                group, new AlwaysEnqueueOnError(), basePolicy.sequencePolicy());
        // commandSide is always false: v1 is event-side only (guarded in the ctor).
        return new ProcessingGroup(group, bus, bus.getSerializer(), false, capturing,
                bus.getConsumers(group), retryPolicy);
    }

    /**
     * Forces any handler exception during retry into {@link #captured} instead of
     * letting the group's real (default IGNORE) policy swallow it.
     */
    private static final class AlwaysEnqueueOnError extends DlqEnqueuePolicy {
        @Override
        public DlqEnqueueDecisionResult shouldEnqueue(InternalMessage message, Throwable error) {
            return error != null ? DlqEnqueueDecisionResult.enqueue()
                    : DlqEnqueueDecisionResult.doNotEnqueue();
        }
    }

    /**
     * Throwaway store for the retry's isolated processing group. Never blocks
     * (so dispatch runs) and captures the single re-failure item if one occurs.
     */
    private static final class CapturingDlqStore implements DlqStore {
        private DlqItem captured;

        @Override
        public boolean hasBlockedItems(String sequenceId) {
            return false;
        }

        @Override
        public void addItem(DlqItem item, String sequenceId) {
            captured = item;
        }

        @Override
        public void evictFirst(String sequenceId) {
        }

        @Override
        public List<DlqItem> listItems(String sequenceId) {
            return List.of();
        }

        @Override
        public Optional<DlqItem> getItem(UUID id) {
            return Optional.empty();
        }

        @Override
        public void updateStatus(UUID id, DlqItemStatus status) {
        }

        @Override
        public void updateItem(DlqItem item) {
        }

        @Override
        public void removeItem(UUID id) {
        }

        @Override
        public void clear() {
        }
    }
}
