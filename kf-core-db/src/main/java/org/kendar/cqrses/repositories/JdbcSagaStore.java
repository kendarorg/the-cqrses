package org.kendar.cqrses.repositories;

import org.kendar.cqrses.db.Db;
import org.kendar.cqrses.db.RowMapper;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.pg.SegmentCalculator;
import org.kendar.cqrses.serialization.MessageSerializer;

import java.util.*;

/**
 * JDBC {@link org.kendar.cqrses.repositories.SagaStore} extending
 * {@link BaseSagaStore} (so {@code @SagaId} / {@code @SagaHandler} correlation
 * reflection is inherited). Mirrors {@link InMemorySagaStore}:
 * {@code saga_instance} holds the serialized saga, {@code saga_correlation} is
 * the node-local {@code (type, value) -> sagaId} index.
 * <p>
 * On re-store it replays the in-memory <em>owned-values diff</em> — read the
 * saga's current correlation values from {@code saga_correlation} (rather than a
 * heap map), drop the {@code (type, value)} rows it no longer carries, and
 * upsert the new ones — so a stale value can't keep shadowing whichever saga
 * later claims it.
 * <p>
 * <b>Collisions resolve to silent last-writer-wins.</b> Two sagas legitimately
 * claiming the same {@code (type, corr_value)} converge to "last claimer owns the
 * value"; the store neither detects nor rejects the collision (matching the
 * in-memory reference).
 * <p>
 * <b>The {@code oldValues} SELECT is deliberately not {@code FOR UPDATE}.</b> It is
 * safe because a saga never re-stores concurrently with itself
 * ({@code seg(sagaId)} maps to one worker, and drain-before-release rules out two
 * owners), so cross-saga safety rests entirely on the {@code saga_id}-filtered
 * DELETE — the same single-writer-per-segment invariant as the rest of the module.
 * The diff is idempotent under at-least-once re-delivery.
 */
public class JdbcSagaStore extends BaseSagaStore {

    private final Db db;

    public JdbcSagaStore(Db db) {
        this.db = db;
    }

    private static MessageSerializer<?, ?> serializer() {
        return GlobalRegistry.get(MessageSerializer.class);
    }

    private static int segment(String id) {
        return SegmentCalculator.calculateSegment(id);
    }

    private static SagaInstance instance(String id, String type, String correlationId, byte[] content) {
        SagaInstance s = new SagaInstance();
        s.setId(id);
        s.setType(type);
        s.setCorrelationId(correlationId);
        s.setContent(content);
        return s;
    }

    @Override
    public void storeSaga(Object saga) {
        Objects.requireNonNull(saga, "saga");
        String id = extractSagaId(saga);
        if (id == null) {
            throw new IllegalStateException(
                    "Saga " + saga.getClass().getName() + " has a null @SagaId value");
        }
        String type = extractSagaType(saga);
        int seg = segment(id);
        byte[] content = serializer().serialize(saga);

        List<Correlation> newCorrelations = extractCorrelations(saga);
        // Canonical single correlationId = first correlation value (if any); lookups
        // by correlation override this on the returned view.
        String canonical = newCorrelations.isEmpty() ? null : newCorrelations.get(0).value();

        db.update("INSERT INTO saga_instance(saga_id, type, segment, correlation_id, content) " +
                        "VALUES (?,?,?,?,?) " +
                        "ON DUPLICATE KEY UPDATE segment = VALUES(segment), " +
                        "correlation_id = VALUES(correlation_id), content = VALUES(content)",
                id, type, seg, canonical, content);

        Set<String> newValues = new HashSet<>();
        for (Correlation c : newCorrelations) newValues.add(c.value());

        Set<String> oldValues = new HashSet<>(db.queryForList(
                "SELECT corr_value FROM saga_correlation WHERE type = ? AND saga_id = ?",
                String.class, type, id));

        for (String v : oldValues) {
            if (!newValues.contains(v)) {
                db.update("DELETE FROM saga_correlation WHERE type = ? AND corr_value = ? AND saga_id = ?",
                        type, v, id);
            }
        }
        for (String v : newValues) {
            db.update("INSERT INTO saga_correlation(type, corr_value, saga_id, segment) " +
                            "VALUES (?,?,?,?) " +
                            "ON DUPLICATE KEY UPDATE saga_id = VALUES(saga_id), segment = VALUES(segment)",
                    type, v, id, seg);
        }
    }

    @Override
    public void deleteSaga(Object saga) {
        Objects.requireNonNull(saga, "saga");
        String id = extractSagaId(saga);
        if (id == null) return;
        String type = extractSagaType(saga);
        db.update("DELETE FROM saga_correlation WHERE type = ? AND saga_id = ?", type, id);
        db.update("DELETE FROM saga_instance WHERE type = ? AND saga_id = ?", type, id);
    }

    @Override
    public Optional<SagaInstance> loadSaga(String sagaId) {
        if (sagaId == null) return Optional.empty();
        SagaInstance s = db.queryForObject(
                "SELECT saga_id, type, correlation_id, content FROM saga_instance WHERE saga_id = ? LIMIT 1",
                instanceMapper, sagaId);
        return Optional.ofNullable(s);
    }

    @Override
    public Optional<SagaInstance> loadSagaByCorrelationId(String correlationId, String type) {
        if (correlationId == null || type == null) return Optional.empty();
        String sagaId = db.queryForObject(
                "SELECT saga_id FROM saga_correlation WHERE type = ? AND corr_value = ?",
                String.class, type, correlationId);
        if (sagaId == null) return Optional.empty();
        byte[] content = db.queryForObject(
                "SELECT content FROM saga_instance WHERE type = ? AND saga_id = ?",
                (rs, rowNum) -> rs.getBytes("content"), type, sagaId);
        if (content == null) return Optional.empty();
        // Reflect the value the caller looked up, not the canonical one.
        return Optional.of(instance(sagaId, type, correlationId, content));
    }

    private final RowMapper<SagaInstance> instanceMapper = (rs, rowNum) ->
            instance(rs.getString("saga_id"), rs.getString("type"),
                    rs.getString("correlation_id"), rs.getBytes("content"));
}
