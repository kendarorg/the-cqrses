package org.kendar.cqrses.repositories;

import org.kendar.cqrses.bus.Context;
import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.db.ConnectionStorage;
import org.kendar.cqrses.db.Db;
import org.kendar.cqrses.db.DbException;
import org.kendar.cqrses.db.RowMapper;
import org.kendar.cqrses.db.UuidBytes;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.exceptions.OptimisticConcurrencyException;
import org.kendar.cqrses.observability.AppendPhase;
import org.kendar.cqrses.observability.Observability;
import org.kendar.cqrses.observability.TraceRecorder;
import org.kendar.cqrses.pg.SegmentCalculator;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.utils.UUIDGenerator;

import java.sql.*;
import java.time.Instant;
import java.util.*;

/**
 * JDBC {@link org.kendar.cqrses.repositories.EventStore} extending
 * {@link BaseEventStore} (so {@code loadAggregate}, snapshot+tail replay and
 * upcasters are inherited unchanged). Implements the four storage primitives
 * against {@code event_entry} / {@code snapshot_entry}.
 * <p>
 * Optimistic concurrency without advisory locks (single-node module). The sole
 * append serialiser is the {@code segment_counter} row lock:
 * <ol>
 *   <li>{@link #lockSegmentCounter} takes {@code SELECT ... FOR UPDATE} on the
 *       segment's counter row and holds it to commit. Because every append in a
 *       segment runs under that one lock, append+version-assign is serialised
 *       <em>per segment</em> — a strict superset of per-aggregate serialisation.
 *       The same lock guards the gap-free {@code segment_seq} the pull tail
 *       depends on (see {@link #lockSegmentCounter} and {@code docs/tricks.md}).</li>
 *   <li>The {@code UNIQUE (aggregate_id, sequence)} constraint is the durable
 *       backstop: a duplicate-sequence insert (a second JVM appending to the same
 *       aggregate before command-forwarding exists, or a bug) throws, which we
 *       translate to the same {@code OptimisticConcurrencyException} the in-memory
 *       store throws.</li>
 * </ol>
 * There is deliberately no in-process per-aggregate {@code synchronized} striped
 * lock: the segment row lock already serialises everything it would have, and the
 * {@code UNIQUE} backstop covers the cross-JVM append race. See {@code docs/tricks.md}.
 * <p>
 * Because the row lock is held to commit, the commit/fsync used to bound each
 * segment's append throughput at {@code 1 / fsync}. The own-connection
 * (synchronous command) path therefore goes through a per-segment
 * {@link SegmentAppendCoalescer}: concurrent appends are folded into one
 * transaction with one commit, releasing each caller only after that commit —
 * durability per acked command is unchanged, the fsync is amortised over the
 * batch. The boundary path ({@code JdbcProcessingGroup}) keeps its inline append.
 */
public class JdbcEventStore extends BaseEventStore {

    private final Db db;
    // Group commit for the own-connection (sendSync) append path: concurrent
    // appends to one segment share a transaction and a single commit/fsync.
    private final SegmentAppendCoalescer coalescer = new SegmentAppendCoalescer(this::writeBatch);
    // Dialect, detected once (lazily, off the poll hot path) and cached. H2 (MODE=MySQL)
    // reports "H2"; a real server reports "MySQL". Drives the loadSegmentsTail fairness split.
    private volatile Boolean mysql;

    public JdbcEventStore(Db db) {
        this.db = db;
    }

    private static MessageSerializer<?, ?> serializer() {
        return GlobalRegistry.get(MessageSerializer.class);
    }

    private final RowMapper<InternalMessage> eventMapper = (rs, rowNum) -> {
        Context c = new Context();
        c.setAggregateId(UuidBytes.fromBytes(rs.getBytes("aggregate_id")));
        c.setAggregateVersion(rs.getLong("sequence"));
        c.setType(rs.getString("event_type"));
        c.setProcessingGroup(rs.getString("processing_group"));
        InternalMessage m = new InternalMessage();
        m.setEvent(true);
        m.setContext(c);
        m.setPayload(rs.getBytes("payload"));
        return m;
    };

    @Override
    public void appendEvents(List<InternalMessage> events) {
        if (events == null || events.isEmpty()) return;

        Map<UUID, List<InternalMessage>> byAggregate = new LinkedHashMap<>();
        for (InternalMessage e : events) {
            UUID aggregateId = e.getContext().getAggregateId();
            if (aggregateId == null) {
                throw new IllegalArgumentException("InternalMessage.context.aggregateId must not be null");
            }
            byAggregate.computeIfAbsent(aggregateId, k -> new ArrayList<>()).add(e);
        }

        // Canonical lock ordering: when one batch touches more than one segment
        // (a multi-aggregate-per-transaction command), acquire the segment_counter
        // FOR UPDATE rows in ascending segment order so two boundary transactions
        // can never hold two segments in opposite order and deadlock. The normal
        // flow is one command -> one aggregate -> one segment, so this is cheap
        // insurance. See docs/tricks.md.
        List<UUID> ordered = new ArrayList<>(byAggregate.keySet());
        ordered.sort(Comparator.comparingInt(SegmentCalculator::calculateSegment));
        if (ConnectionStorage.isBound()) {
            // A transaction boundary (JdbcProcessingGroup) has bound a connection to this
            // thread: the append must run as part of the boundary's transaction and leave
            // commit/rollback/close to the boundary. Cannot be coalesced with other threads.
            for (UUID aggregateId : ordered) {
                appendOnBoundary(aggregateId, byAggregate.get(aggregateId));
            }
            return;
        }
        // Own-connection path (synchronous commands): group commit per segment — the
        // coalescer folds concurrent appends to one segment into a single transaction
        // with a single commit/fsync, releasing each caller only after that commit.
        for (UUID aggregateId : ordered) {
            List<InternalMessage> aggregateEvents = byAggregate.get(aggregateId);
            coalescer.append(SegmentCalculator.calculateSegment(aggregateId), aggregateId,
                    groupOf(aggregateEvents), aggregateEvents);
        }
    }

    private static String groupOf(List<InternalMessage> aggregateEvents) {
        String group = aggregateEvents.get(0).getContext().getProcessingGroup();
        return group == null ? BaseEventStore.DEFAULT_GROUP : group;
    }

    /**
     * Append inside the boundary's transaction on the thread-bound connection.
     * No commit and no rollback here — the whole unit of work succeeds or fails
     * with the boundary ({@code JdbcProcessingGroup.transactionEnd}).
     */
    private void appendOnBoundary(UUID aggregateId, List<InternalMessage> aggregateEvents) {
        int segment = SegmentCalculator.calculateSegment(aggregateId);
        // Phase marks split the append wall-clock (already reported as
        // kf.events.append) into its serialisation points; each phase(...)
        // call is a no-op-cheap Observability + TraceRecorder emit.
        Observability.get().onAppendInFlight(segment, +1);
        long mark = System.nanoTime();
        try {
            Connection conn = db.connection();
            mark = phase(AppendPhase.CONN, mark, 0);
            // Reserve a contiguous, gap-free block of segment_seq for this aggregate's
            // batch under the segment_counter row lock (held to commit). This lock is
            // also the append serialiser: holding it across the MAX(sequence) read makes
            // currentMax consistent, so per-aggregate OCC needs no extra in-process lock.
            long nextSeq = lockSegmentCounter(conn, segment);
            phase(AppendPhase.LOCK, mark, 0);
            nextSeq = insertAggregateEvents(conn, segment, groupOf(aggregateEvents), aggregateId,
                    aggregateEvents, nextSeq);
            mark = System.nanoTime();
            advanceCounter(conn, segment, nextSeq);
            phase(AppendPhase.COUNTER, mark, 0);
        } catch (SQLException sql) {
            throw translate(aggregateId, sql);
        } finally {
            Observability.get().onAppendInFlight(segment, -1);
        }
    }

    /**
     * One coalesced batch for one segment, written by whichever caller thread
     * leads it (see {@link SegmentAppendCoalescer}): one transaction, the segment
     * row lock taken once, every request's rows inserted, one counter advance,
     * one commit. Completes / fails every request — never throws.
     * <p>
     * Failure isolation, in order of blast radius:
     * <ul>
     *   <li><b>Version-check OCC mismatch</b> — detected by validation before any
     *       statement for that request runs, so the transaction is untouched: the
     *       request fails alone, the batch goes on.</li>
     *   <li><b>Any SQL failure</b> (duplicate-sequence from a cross-JVM race, a
     *       dropped connection, ...) — the transaction may be poisoned, so the
     *       whole batch rolls back (nothing was acked yet) and every pending
     *       request is retried individually in its own transaction: innocents
     *       land durably, only the real offender fails.</li>
     * </ul>
     */
    private void writeBatch(int segment, List<SegmentAppendCoalescer.Req> batch) {
        Connection conn = null;
        try {
            long mark = System.nanoTime();
            conn = db.connection();
            conn.setAutoCommit(false);
            mark = phase(AppendPhase.CONN, mark, 0);
            long startSeq = lockSegmentCounter(conn, segment);
            phase(AppendPhase.LOCK, mark, 0);
            long nextSeq = startSeq;
            for (SegmentAppendCoalescer.Req req : batch) {
                try {
                    nextSeq = insertAggregateEvents(conn, segment, req.group(), req.aggregateId(),
                            req.events(), nextSeq);
                } catch (OptimisticConcurrencyException occ) {
                    req.fail(occ);
                }
            }
            if (nextSeq == startSeq) {
                // Every request failed validation: nothing to write, release the row lock.
                conn.rollback();
            } else {
                mark = System.nanoTime();
                advanceCounter(conn, segment, nextSeq);
                mark = phase(AppendPhase.COUNTER, mark, 0);
                conn.commit();
                phase(AppendPhase.COMMIT, mark, 0);
                Observability.get().onAppendBatch(batch.size(), (int) (nextSeq - startSeq));
            }
            for (SegmentAppendCoalescer.Req req : batch) {
                req.complete();
            }
        } catch (SQLException | RuntimeException ex) {
            rollback(conn);
            closeQuietly(conn);
            conn = null;
            retryIndividually(batch);
        } finally {
            closeQuietly(conn);
        }
    }

    /**
     * Post-rollback fallback: re-run each still-pending request of a failed batch
     * in its own transaction. Requests already failed by the version check keep
     * their verdict; the rest are restored to their as-requested versions (the
     * rolled-back attempt may have assigned versions that are stale by now) and
     * re-executed, so a single poisoned request cannot take innocents down.
     */
    private void retryIndividually(List<SegmentAppendCoalescer.Req> batch) {
        for (SegmentAppendCoalescer.Req req : batch) {
            if (req.isDone()) continue;
            req.restoreRequestedVersions();
            try {
                appendAggregateOwnTxn(req.aggregateId(), req.events());
                req.complete();
            } catch (RuntimeException ex) {
                req.fail(ex);
            }
        }
    }

    /** A single-aggregate append in its own transaction (the pre-coalescer shape). */
    private void appendAggregateOwnTxn(UUID aggregateId, List<InternalMessage> aggregateEvents) {
        int segment = SegmentCalculator.calculateSegment(aggregateId);
        Connection conn = null;
        try {
            long mark = System.nanoTime();
            conn = db.connection();
            conn.setAutoCommit(false);
            mark = phase(AppendPhase.CONN, mark, 0);
            long nextSeq = lockSegmentCounter(conn, segment);
            phase(AppendPhase.LOCK, mark, 0);
            nextSeq = insertAggregateEvents(conn, segment, groupOf(aggregateEvents), aggregateId,
                    aggregateEvents, nextSeq);
            mark = System.nanoTime();
            advanceCounter(conn, segment, nextSeq);
            mark = phase(AppendPhase.COUNTER, mark, 0);
            conn.commit();
            phase(AppendPhase.COMMIT, mark, 0);
        } catch (SQLException sql) {
            rollback(conn);
            throw translate(aggregateId, sql);
        } catch (RuntimeException ex) {
            rollback(conn);
            throw ex;
        } finally {
            closeQuietly(conn);
        }
    }

    /**
     * Validates and assigns aggregate versions for {@code events} against the
     * stream's {@code MAX(sequence)}, then inserts the rows assigning consecutive
     * {@code segment_seq} from {@code nextSeq}; returns the advanced value. The
     * validation pass executes <b>no statements</b>, so an
     * {@link OptimisticConcurrencyException} from it leaves the surrounding
     * transaction clean — which is what lets a coalesced batch fail one request
     * and keep the others.
     */
    private static long insertAggregateEvents(Connection conn, int segment, String group,
                                              UUID aggregateId, List<InternalMessage> events,
                                              long nextSeq) throws SQLException {
        long mark = System.nanoTime();
        long currentMax = currentMax(conn, aggregateId);
        mark = phase(AppendPhase.CURRENT_MAX, mark, 0);
        for (InternalMessage msg : events) {
            long version = msg.getContext().getAggregateVersion();
            if (version == -1L) {
                // -1 = "assign next on append": store a concrete monotonic
                // version so snapshot tail replay never skips the event.
                currentMax++;
                msg.getContext().setAggregateVersion(currentMax);
            } else {
                if (version != currentMax + 1) {
                    throw new OptimisticConcurrencyException(
                            "Optimistic concurrency violation for aggregate " + aggregateId +
                                    ": expected version " + (currentMax + 1) + " but got " + version);
                }
                currentMax = version;
            }
        }
        long now = Instant.now().toEpochMilli();
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO event_entry(id, aggregate_id, processing_group, segment, segment_seq, event_type, sequence, payload, created_at) " +
                        "VALUES (?,?,?,?,?,?,?,?,?)")) {
            for (InternalMessage msg : events) {
                long segSeq = nextSeq++;
                ps.setBytes(1, UuidBytes.toBytes(UUIDGenerator.newUuid()));
                ps.setBytes(2, UuidBytes.toBytes(aggregateId));
                ps.setString(3, group);
                ps.setInt(4, segment);
                ps.setLong(5, segSeq);
                ps.setString(6, msg.getContext().getType());
                ps.setLong(7, msg.getContext().getAggregateVersion());
                ps.setBytes(8, msg.getPayload());
                ps.setLong(9, now);
                ps.executeUpdate();
                msg.setSegmentSeq(segSeq);
                msg.setCreatedAt(now);
            }
        }
        phase(AppendPhase.INSERT, mark, events.size());
        return nextSeq;
    }

    /** Advance the counter past the highest segment_seq this transaction assigned. */
    private static void advanceCounter(Connection conn, int segment, long nextSeq) throws SQLException {
        try (PreparedStatement up = conn.prepareStatement(
                "UPDATE segment_counter SET next_seq = ? WHERE segment = ?")) {
            up.setLong(1, nextSeq);
            up.setInt(2, segment);
            up.executeUpdate();
        }
    }

    /**
     * The store's exception contract: an integrity violation (the
     * {@code UNIQUE(aggregate_id, sequence)} backstop firing on a cross-JVM
     * append race, or any SQLState class 23) is an
     * {@link OptimisticConcurrencyException}; anything else a {@link DbException}.
     */
    private static RuntimeException translate(UUID aggregateId, SQLException sql) {
        if (sql instanceof SQLIntegrityConstraintViolationException
                || (sql.getSQLState() != null && sql.getSQLState().startsWith("23"))) {
            return new OptimisticConcurrencyException(
                    "Optimistic concurrency violation for aggregate " + aggregateId +
                            ": duplicate sequence", sql);
        }
        return new DbException(sql);
    }

    /**
     * Emit one append phase to the metrics seam and the sampled trace, returning
     * the new mark. On exception paths phases simply stop being emitted where the
     * throw happened — no behaviour change.
     */
    private static long phase(String name, long mark, long detail) {
        long now = System.nanoTime();
        Observability.get().onAppendPhase(name, now - mark);
        if (TraceRecorder.active()) {
            TraceRecorder.stage("append." + name, now - mark, detail);
        }
        return now;
    }

    private static long currentMax(Connection conn, UUID aggregateId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COALESCE(MAX(sequence), -1) FROM event_entry WHERE aggregate_id = ?")) {
            ps.setBytes(1, UuidBytes.toBytes(aggregateId));
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    /**
     * Ensures the {@code segment_counter} row for {@code segment} exists, locks it
     * with {@code SELECT ... FOR UPDATE} (in the H2 &cap; MySQL intersection), and
     * returns the next segment_seq to assign. The caller advances {@code next_seq}
     * after the inserts; the whole thing is in one transaction, so an OCC rollback
     * undoes the increment too.
     * <p>
     * <b>Gap-free, commit-order {@code segment_seq} is a hard correctness
     * invariant, not a nicety.</b> The pull pump
     * ({@code SegmentProcessor}) polls {@code segment_seq > cursor ORDER BY
     * segment_seq} and advances a single strictly-greater watermark, so if
     * {@code N+1} could ever become visible before {@code N} the watermark would
     * step past {@code N} and that event would be <em>permanently skipped</em> into
     * its projections — the classic "you can't poll a bare auto-increment" hazard.
     * Holding this row lock to commit makes assignment order == commit order, which
     * is exactly what an auto-increment / DB sequence / {@code MAX()+1}-without-lock
     * cannot guarantee. Rollback gaps (a reserved number that never commits) are
     * tolerable; out-of-commit-order assignment is not. Do not replace this with a
     * cheaper counter.
     */
    private static long lockSegmentCounter(Connection conn, int segment) throws SQLException {
        // No-op upsert to guarantee the row exists, racing inserts collapse on the PK.
        try (PreparedStatement ins = conn.prepareStatement(
                "INSERT INTO segment_counter(segment, next_seq) VALUES (?, 0) " +
                        "ON DUPLICATE KEY UPDATE next_seq = next_seq")) {
            ins.setInt(1, segment);
            ins.executeUpdate();
        }
        try (PreparedStatement sel = conn.prepareStatement(
                "SELECT next_seq FROM segment_counter WHERE segment = ? FOR UPDATE")) {
            sel.setInt(1, segment);
            try (ResultSet rs = sel.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private final RowMapper<InternalMessage> tailMapper = (rs, rowNum) -> {
        Context c = new Context();
        c.setAggregateId(UuidBytes.fromBytes(rs.getBytes("aggregate_id")));
        c.setAggregateVersion(rs.getLong("sequence"));
        c.setType(rs.getString("event_type"));
        c.setProcessingGroup(rs.getString("processing_group"));
        InternalMessage m = new InternalMessage();
        m.setEvent(true);
        m.setContext(c);
        m.setPayload(rs.getBytes("payload"));
        m.setSegmentSeq(rs.getLong("segment_seq"));
        m.setCreatedAt(rs.getLong("created_at"));
        return m;
    };

    @Override
    public List<InternalMessage> loadSegmentTail(int segment, long afterSeq, int limit) {
        return db.query(
                "SELECT * FROM event_entry WHERE segment = ? AND segment_seq > ? ORDER BY segment_seq LIMIT ?",
                tailMapper, segment, afterSeq, limit);
    }

    @Override
    public List<InternalMessage> loadSegmentsTail(Map<Integer, Long> afterSeqBySegment, int limit) {
        if (afterSeqBySegment == null || afterSeqBySegment.isEmpty()) {
            return List.of();
        }
        // OR-of-ANDs over each owned segment's own cursor — segment_seq is per-segment,
        // so there is no single global cursor. ORDER BY segment, segment_seq keeps the
        // tail exact within each segment (projections need that); cross-segment order is free.
        StringBuilder where = new StringBuilder();
        int n = afterSeqBySegment.size();
        for (Map.Entry<Integer, Long> e : afterSeqBySegment.entrySet()) {
            if (where.length() > 0) where.append(" OR ");
            where.append("(segment = ? AND segment_seq > ?)");
        }

        if (isMysql()) {
            // Fairness on real MySQL (prod): a single global `LIMIT n` with `segment ASC`
            // lets one backlogged low-numbered segment consume the whole batch and starve
            // the others. Cap each segment to its fair share via a window function so every
            // owned segment makes progress every poll. MySQL 8.0+ required (window funcs).
            int perSegment = Math.max(1, (limit + n - 1) / n);
            Object[] args = new Object[n * 2 + 2];
            int i = 0;
            for (Map.Entry<Integer, Long> e : afterSeqBySegment.entrySet()) {
                args[i++] = e.getKey();
                args[i++] = e.getValue();
            }
            args[i++] = perSegment;
            args[i] = limit;
            return db.query(
                    "SELECT * FROM (" +
                            "  SELECT event_entry.*, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY segment_seq) AS rn " +
                            "  FROM event_entry WHERE " + where +
                            ") ranked WHERE rn <= ? ORDER BY segment, segment_seq LIMIT ?",
                    tailMapper, args);
        }

        // H2 (dev/test only): the cheap global LIMIT. It can starve a colder segment under
        // a heavy low-numbered backlog, so H2 is no longer a faithful proxy for fairness —
        // that property is proven only against real MySQL (see the fairness IT).
        Object[] args = new Object[n * 2 + 1];
        int i = 0;
        for (Map.Entry<Integer, Long> e : afterSeqBySegment.entrySet()) {
            args[i++] = e.getKey();
            args[i++] = e.getValue();
        }
        args[i] = limit;
        return db.query(
                "SELECT * FROM event_entry WHERE " + where + " ORDER BY segment, segment_seq LIMIT ?",
                tailMapper, args);
    }

    /**
     * True on a real MySQL server. Detected once via JDBC metadata and cached — never
     * re-probed in the poll hot path. H2 reports {@code H2} even in {@code MODE=MySQL}.
     */
    private boolean isMysql() {
        Boolean cached = mysql;
        if (cached != null) return cached;
        synchronized (this) {
            if (mysql != null) return mysql;
            try (Connection c = db.connection()) {
                String product = c.getMetaData().getDatabaseProductName();
                mysql = product != null && product.toLowerCase(Locale.ROOT).contains("mysql");
            } catch (SQLException e) {
                throw new DbException(e);
            }
            return mysql;
        }
    }

    @Override
    public List<InternalMessage> loadSegmentTypeTail(int sourceSegment, Set<String> eventTypes,
                                                     long afterSeq, int limit) {
        if (eventTypes == null || eventTypes.isEmpty()) {
            return List.of();
        }
        String placeholders = String.join(",", Collections.nCopies(eventTypes.size(), "?"));
        Object[] args = new Object[eventTypes.size() + 3];
        int i = 0;
        args[i++] = sourceSegment;
        for (String t : eventTypes) {
            args[i++] = t;
        }
        args[i++] = afterSeq;
        args[i] = limit;
        return db.query(
                "SELECT * FROM event_entry WHERE segment = ? AND event_type IN (" + placeholders + ") " +
                        "AND segment_seq > ? ORDER BY segment_seq LIMIT ?",
                tailMapper, args);
    }

    @Override
    public List<InternalMessage> loadEvents(UUID aggregateId, long fromVersion) {
        if (fromVersion == -1L) {
            return db.query("SELECT * FROM event_entry WHERE aggregate_id = ? ORDER BY sequence",
                    eventMapper, UuidBytes.toBytes(aggregateId));
        }
        return db.query("SELECT * FROM event_entry WHERE aggregate_id = ? AND sequence > ? ORDER BY sequence",
                eventMapper, UuidBytes.toBytes(aggregateId), fromVersion);
    }

    @Override
    public Optional<AggregateSnapshot> loadSnapshot(UUID aggregateId) {
        AggregateSnapshot snap = db.queryForObject(
                "SELECT aggregate_id, sequence, payload, schema_version, snapshot_type " +
                        "FROM snapshot_entry WHERE aggregate_id = ?",
                (rs, rowNum) -> {
                    AggregateSnapshot s = new AggregateSnapshot();
                    s.setAggregateId(UuidBytes.fromBytes(rs.getBytes("aggregate_id")));
                    s.setAggregateVersion(rs.getInt("sequence"));
                    s.setSnapshot(rs.getBytes("payload"));
                    s.setSchemaVersion(rs.getLong("schema_version"));
                    s.setSnapshotType(rs.getString("snapshot_type"));
                    return s;
                }, UuidBytes.toBytes(aggregateId));
        return Optional.ofNullable(snap);
    }

    @Override
    public void storeSnapshot(UUID aggregateId, Object snapshotPayload, long schemaVersion, long aggregateVersion) {
        if (snapshotPayload == null) {
            throw new IllegalStateException(
                    "Snapshot payload for aggregate " + aggregateId + " must not be null");
        }
        String group = BaseEventStore.groupOf(snapshotPayload.getClass());
        int segment = SegmentCalculator.calculateSegment(aggregateId);
        int version;
        if (aggregateVersion >= 0) {
            // Explicit stamp (the automatic trigger's path): exactly the last
            // version of the batch the caller just folded + appended.
            version = (int) aggregateVersion;
        } else {
            // Stamp the current event-stream max — the highest event already folded
            // into the state being snapshotted. No per-aggregate lock: snapshots are
            // best-effort (CLAUDE.md §5) and the old synchronized never protected
            // this committed-state read against a racing append anyway.
            Long max = db.queryForObject(
                    "SELECT COALESCE(MAX(sequence), -1) FROM event_entry WHERE aggregate_id = ?",
                    Long.class, UuidBytes.toBytes(aggregateId));
            version = (int) (max == null ? -1L : max);
        }
        byte[] payload = serializer().serialize(snapshotPayload);
        db.update("INSERT INTO snapshot_entry(aggregate_id, processing_group, segment, sequence, payload, " +
                        "schema_version, snapshot_type, updated_at) " +
                        "VALUES (?,?,?,?,?,?,?,?) " +
                        "ON DUPLICATE KEY UPDATE processing_group = VALUES(processing_group), " +
                        "segment = VALUES(segment), sequence = VALUES(sequence), " +
                        "payload = VALUES(payload), schema_version = VALUES(schema_version), " +
                        "snapshot_type = VALUES(snapshot_type), updated_at = VALUES(updated_at)",
                UuidBytes.toBytes(aggregateId), group, segment, version, payload,
                schemaVersion, snapshotPayload.getClass().getSimpleName(),
                Instant.now().toEpochMilli());
    }

    private static void rollback(Connection conn) {
        if (conn == null) return;
        try {
            conn.rollback();
        } catch (SQLException ignored) {
            // best-effort; the originating exception is what matters
        }
    }

    private static void closeQuietly(Connection conn) {
        if (conn == null) return;
        try {
            conn.setAutoCommit(true);
        } catch (SQLException ignored) {
        }
        try {
            conn.close();
        } catch (SQLException ignored) {
        }
    }
}
