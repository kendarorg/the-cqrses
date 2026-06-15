package org.kendar.cqrses.repositories;

import org.kendar.cqrses.dlq.DlqItem;
import org.kendar.cqrses.dlq.DlqItemStatus;
import org.kendar.cqrses.dlq.DlqStore;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class InMemoryDlqStore implements DlqStore {
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<DlqItem>> items = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, DlqItem> idMapping = new ConcurrentHashMap<>();
    @Override
    public boolean hasBlockedItems(String sequenceId) {
        return items.containsKey(sequenceId) && !items.get(sequenceId).isEmpty();
    }

    @Override
    public void evictFirst(String sequenceId) {
        var queue = items.get(sequenceId);
        if (queue == null) return;
        // No-op on an empty/absent sequence, mirroring JDBC's set-based
        // DELETE … LIMIT 1 (which silently affects zero rows). poll() returning
        // null here previously NPE'd on item.getId().
        var item = queue.poll();
        if (item == null) {
            items.remove(sequenceId, queue);
            return;
        }
        idMapping.remove(item.getId());
        // Prune the now-empty queue so containsKey stays honest and drained
        // sequences don't leak map entries.
        if (queue.isEmpty()) items.remove(sequenceId, queue);
    }

    @Override
    public void addItem( DlqItem item,String sequenceId) {
        if(!items.containsKey(sequenceId))items.put(sequenceId,new ConcurrentLinkedQueue<>());
        items.get(sequenceId).add(item);
        idMapping.put(item.getId(),item);
    }

    @Override
    public List<DlqItem> listItems(String sequenceId) {
        if(!items.containsKey(sequenceId))return List.of();
        return items.get(sequenceId).stream().toList();
    }

    @Override
    public Optional<DlqItem> getItem(UUID id) {
        if(idMapping.containsKey(id))return Optional.of(idMapping.get(id));
        return Optional.empty();
    }

    @Override
    public void updateStatus(UUID id, DlqItemStatus status) {
        if(idMapping.containsKey(id)){
            var item = idMapping.get(id);
            item.setStatus(status);
        }
    }

    @Override
    public void updateItem(DlqItem item) {
        if (!idMapping.containsKey(item.getId())) return;
        idMapping.put(item.getId(), item);
        var queue = items.get(item.getSequenceId());
        if (queue == null) return;
        // Replace the entry in place, preserving FIFO order. The old remove()+add()
        // moved the item to the tail, corrupting head-of-line ordering when a failed
        // retry left an item PENDING in a multi-item sequence.
        var rebuilt = new ConcurrentLinkedQueue<DlqItem>();
        for (var existing : queue) {
            rebuilt.add(existing.getId().equals(item.getId()) ? item : existing);
        }
        items.put(item.getSequenceId(), rebuilt);
    }

    @Override
    public void removeItem(UUID id) {
        var item = idMapping.remove(id);
        if (item == null) return;
        var queue = items.get(item.getSequenceId());
        // DlqItem.equals is id+sequenceId+processingGroup, so this removes the right
        // entry wherever it sits in the queue, leaving the others in order.
        if (queue != null) {
            queue.remove(item);
            // Prune the now-empty queue so containsKey stays honest and a later
            // evictFirst no-ops instead of finding a stale empty entry.
            if (queue.isEmpty()) items.remove(item.getSequenceId(), queue);
        }
    }

    @Override
    public void clear() {
        items.clear();
        idMapping.clear();
    }

}
