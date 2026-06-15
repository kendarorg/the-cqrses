package org.kendar.pfm.read;

import org.kendar.pfm.domain.OpType;
import org.kendar.pfm.web.dto.OperationView;
import org.kendar.pfm.web.dto.Summary;
import org.kendar.pfm.web.dto.TagSummary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

/**
 * Durable read store for the {@code pfm_operation} table. Writes are insert-ignore on the
 * {@code op_id} primary key so a redelivered {@code OperationRecorded} event can never double-count;
 * totals are SQL aggregates computed at query time, so they cannot drift.
 *
 * <p>Like {@link UserReadStore}, a plain Spring bean injected into {@link LedgerProjection} as a
 * non-first handler parameter, resolved + pre-warmed via the kf-spring bridge.
 */
@Repository
public class OperationReadStore {

    private final JdbcTemplate jdbc;

    public OperationReadStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Idempotent append: INSERT IGNORE on the op_id PK. */
    public void record(UUID opId, UUID userId, OpType type, long amount, String tag, long epochMillis) {
        jdbc.update(
                "INSERT IGNORE INTO pfm_operation(op_id, user_id, op_type, amount, tag, ts) "
                        + "VALUES (?, ?, ?, ?, ?, ?)",
                Uuids.toBytes(opId), Uuids.toBytes(userId), type.name(), amount, tag, epochMillis);
    }

    public Summary summary(UUID userId) {
        long in = sum(userId, OpType.IN);
        long out = sum(userId, OpType.OUT);
        return new Summary(in, out, in - out);
    }

    private long sum(UUID userId, OpType type) {
        Long s = jdbc.queryForObject(
                "SELECT COALESCE(SUM(amount), 0) FROM pfm_operation WHERE user_id = ? AND op_type = ?",
                Long.class, Uuids.toBytes(userId), type.name());
        return s == null ? 0L : s;
    }

    public List<TagSummary> byTag(UUID userId) {
        return jdbc.query(
                "SELECT tag, "
                        + "COALESCE(SUM(CASE WHEN op_type = 'IN' THEN amount ELSE 0 END), 0) AS in_total, "
                        + "COALESCE(SUM(CASE WHEN op_type = 'OUT' THEN amount ELSE 0 END), 0) AS out_total "
                        + "FROM pfm_operation WHERE user_id = ? GROUP BY tag ORDER BY tag",
                (rs, i) -> {
                    long in = rs.getLong("in_total");
                    long out = rs.getLong("out_total");
                    return new TagSummary(rs.getString("tag"), in, out, in - out);
                },
                Uuids.toBytes(userId));
    }

    public List<OperationView> recent(UUID userId, int limit) {
        return jdbc.query(
                "SELECT op_id, op_type, amount, tag, ts FROM pfm_operation "
                        + "WHERE user_id = ? ORDER BY ts DESC LIMIT ?",
                (rs, i) -> new OperationView(
                        Uuids.fromBytes(rs.getBytes("op_id")).toString(),
                        rs.getString("op_type"),
                        rs.getLong("amount"),
                        rs.getString("tag"),
                        rs.getLong("ts")),
                Uuids.toBytes(userId), limit);
    }
}
