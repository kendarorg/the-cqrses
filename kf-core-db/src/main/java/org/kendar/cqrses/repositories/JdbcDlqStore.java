package org.kendar.cqrses.repositories;

import org.kendar.cqrses.bus.Context;
import org.kendar.cqrses.db.Db;
import org.kendar.cqrses.db.RowMapper;
import org.kendar.cqrses.db.UuidBytes;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.dlq.DlqItem;
import org.kendar.cqrses.dlq.DlqItemStatus;
import org.kendar.cqrses.dlq.DlqStore;
import org.kendar.cqrses.serialization.MessageSerializer;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JDBC {@link DlqStore} over the single {@code dlq_item} table. Mirrors
 * {@link InMemoryDlqStore} semantics: a per-{@code sequenceId} FIFO whose order
 * survives {@code updateItem} and from which any position can be removed.
 * <p>
 * FIFO is carried by a monotonic {@code ordinal} column assigned per
 * {@code sequence_id} — {@code updateItem} never touches {@code ordinal}, so a
 * retry that leaves an item PENDING keeps its head-of-line position (the in-SQL
 * equivalent of the in-memory rebuild-queue trick). Ordinal assignment is
 * {@code MAX(ordinal)+1}; it is racy in theory but safe by the module-wide
 * single-writer-per-segment invariant (a given {@code sequence_id}'s letters are
 * produced by one segment owner), so no in-process per-sequence lock and no
 * {@code UNIQUE(sequence_id, ordinal)} are needed. See {@code docs/tricks.md}.
 * <p>
 * <b>In-JVM object identity.</b> Like {@link InMemoryDlqStore}'s {@code idMapping},
 * a heap identity cache makes {@code getItem}/{@code listItems} return the
 * <em>same</em> {@link DlqItem} instance that was {@code addItem}-ed within this
 * JVM, and {@code updateStatus} mutates that live instance. A {@code DlqManager}
 * (and the integration suite) hold a reference to the letter and observe status
 * transitions through it — the row is the durable copy, the cached instance is
 * the shared identity. The cache is rebuilt from the table after a restart, when
 * no live references can exist.
 */
public class JdbcDlqStore implements DlqStore {

    private final Db db;
    private final ConcurrentHashMap<UUID, DlqItem> identityCache = new ConcurrentHashMap<>();

    public JdbcDlqStore(Db db) {
        this.db = db;
    }

    private static MessageSerializer<?, ?> serializer() {
        return GlobalRegistry.get(MessageSerializer.class);
    }

    // The processing Context is persisted through the registered MessageSerializer
    // (JSON), exactly like every other payload — the JSR-310 module on
    // JacksonMessageSerializer lets the Instant timestamp round-trip, so there is
    // no longer a separate Java-binary path here.
    private static byte[] serializeContext(Context ctx) {
        if (ctx == null) return null;
        return serializer().serialize(ctx);
    }

    private static Context deserializeContext(byte[] bytes) {
        if (bytes == null) return null;
        return serializer().deserialize(bytes, Context.class);
    }

    private static Long epoch(Instant i) {
        return i == null ? null : i.toEpochMilli();
    }

    private final RowMapper<DlqItem> mapper = (rs, rowNum) -> {
        DlqItem item = new DlqItem();
        item.setId(UuidBytes.fromBytes(rs.getBytes("id")));
        item.setSequenceId(rs.getString("sequence_id"));
        item.setProcessingGroup(rs.getString("processing_group"));
        item.setEventType(rs.getString("event_type"));
        item.setAggregateId(UuidBytes.fromBytes(rs.getBytes("aggregate_id")));
        item.setSerializedEvent(rs.getBytes("serialized_event"));
        item.setProcessingContext(deserializeContext(rs.getBytes("processing_context")));
        item.setStatus(DlqItemStatus.valueOf(rs.getString("status")));
        item.setRetryCount(rs.getInt("retry_count"));
        item.setErrorMessage(rs.getString("error_message"));
        item.setErrorClass(rs.getString("error_class"));
        item.setStackTrace(rs.getString("stack_trace"));
        long failedAt = rs.getLong("failed_at");
        item.setFailedAt(rs.wasNull() ? null : Instant.ofEpochMilli(failedAt));
        item.setLastRetryErrorMessage(rs.getString("last_retry_error_message"));
        item.setLastRetryErrorClass(rs.getString("last_retry_error_class"));
        item.setLastRetryStackTrace(rs.getString("last_retry_stack_trace"));
        long lastRetryAt = rs.getLong("last_retry_at");
        item.setLastRetryAt(rs.wasNull() ? null : Instant.ofEpochMilli(lastRetryAt));
        return item;
    };

    @Override
    public void addItem(DlqItem item, String sequenceId) {
        // No per-sequence lock: a sequence_id's letters are produced by one segment
        // owner (single-writer-per-segment), so MAX(ordinal)+1 cannot race itself.
        Long maxOrdinal = db.queryForObject(
                "SELECT MAX(ordinal) FROM dlq_item WHERE sequence_id = ?", Long.class, sequenceId);
        long ordinal = (maxOrdinal == null ? 0L : maxOrdinal) + 1L;
        db.insertInto("dlq_item")
                .set("id", UuidBytes.toBytes(item.getId()))
                .set("sequence_id", sequenceId)
                .set("ordinal", ordinal)
                .set("processing_group", item.getProcessingGroup())
                .set("event_type", item.getEventType())
                .set("aggregate_id", UuidBytes.toBytes(item.getAggregateId()))
                .set("serialized_event", item.getSerializedEvent())
                .set("processing_context", serializeContext(item.getProcessingContext()))
                .set("status", item.getStatus() == null ? DlqItemStatus.PENDING.name() : item.getStatus().name())
                .set("retry_count", item.getRetryCount())
                .set("error_message", item.getErrorMessage())
                .set("error_class", item.getErrorClass())
                .set("stack_trace", item.getStackTrace())
                .set("failed_at", epoch(item.getFailedAt()))
                .set("last_retry_error_message", item.getLastRetryErrorMessage())
                .set("last_retry_error_class", item.getLastRetryErrorClass())
                .set("last_retry_stack_trace", item.getLastRetryStackTrace())
                .set("last_retry_at", epoch(item.getLastRetryAt()))
                .execute();
        identityCache.put(item.getId(), item);
    }

    @Override
    public boolean hasBlockedItems(String sequenceId) {
        Long count = db.queryForObject(
                "SELECT COUNT(*) FROM dlq_item WHERE sequence_id = ?", Long.class, sequenceId);
        return count != null && count > 0;
    }

    @Override
    public void evictFirst(String sequenceId) {
        // Remove the lowest-ordinal row for the sequence — the FIFO head, matching
        // the in-memory queue.poll().
        byte[] headId = db.queryForObject(
                "SELECT id FROM dlq_item WHERE sequence_id = ? ORDER BY ordinal LIMIT 1",
                (rs, rowNum) -> rs.getBytes("id"), sequenceId);
        if (headId == null) return;
        db.update("DELETE FROM dlq_item WHERE id = ?", headId);
        identityCache.remove(UuidBytes.fromBytes(headId));
    }

    @Override
    public List<DlqItem> listItems(String sequenceId) {
        List<UUID> ids = db.query(
                "SELECT id FROM dlq_item WHERE sequence_id = ? ORDER BY ordinal",
                (rs, rowNum) -> UuidBytes.fromBytes(rs.getBytes("id")), sequenceId);
        List<DlqItem> out = new ArrayList<>(ids.size());
        for (UUID id : ids) {
            getItem(id).ifPresent(out::add);
        }
        return out;
    }

    @Override
    public Optional<DlqItem> getItem(UUID id) {
        DlqItem cached = identityCache.get(id);
        if (cached != null) return Optional.of(cached);
        DlqItem loaded = db.queryForObject(
                "SELECT * FROM dlq_item WHERE id = ?", mapper, UuidBytes.toBytes(id));
        if (loaded == null) return Optional.empty();
        // After a restart there is no live reference; cache the rehydrated row so
        // it becomes the stable identity for any subsequent action.
        DlqItem prev = identityCache.putIfAbsent(id, loaded);
        return Optional.of(prev != null ? prev : loaded);
    }

    @Override
    public void updateStatus(UUID id, DlqItemStatus status) {
        db.update("UPDATE dlq_item SET status = ? WHERE id = ?", status.name(), UuidBytes.toBytes(id));
        DlqItem cached = identityCache.get(id);
        if (cached != null) cached.setStatus(status);
    }

    @Override
    public void updateItem(DlqItem item) {
        // Update every mutable field but NOT ordinal — FIFO position is preserved.
        db.update("UPDATE dlq_item SET " +
                        "processing_group = ?, event_type = ?, aggregate_id = ?, serialized_event = ?, " +
                        "processing_context = ?, status = ?, retry_count = ?, " +
                        "error_message = ?, error_class = ?, stack_trace = ?, failed_at = ?, " +
                        "last_retry_error_message = ?, last_retry_error_class = ?, " +
                        "last_retry_stack_trace = ?, last_retry_at = ? " +
                        "WHERE id = ?",
                item.getProcessingGroup(), item.getEventType(), UuidBytes.toBytes(item.getAggregateId()),
                item.getSerializedEvent(), serializeContext(item.getProcessingContext()),
                item.getStatus() == null ? null : item.getStatus().name(), item.getRetryCount(),
                item.getErrorMessage(), item.getErrorClass(), item.getStackTrace(), epoch(item.getFailedAt()),
                item.getLastRetryErrorMessage(), item.getLastRetryErrorClass(),
                item.getLastRetryStackTrace(), epoch(item.getLastRetryAt()),
                UuidBytes.toBytes(item.getId()));
        identityCache.put(item.getId(), item);
    }

    @Override
    public void removeItem(UUID id) {
        db.update("DELETE FROM dlq_item WHERE id = ?", UuidBytes.toBytes(id));
        identityCache.remove(id);
    }

    @Override
    public void clear() {
        db.update("DELETE FROM dlq_item");
        identityCache.clear();
    }
}
