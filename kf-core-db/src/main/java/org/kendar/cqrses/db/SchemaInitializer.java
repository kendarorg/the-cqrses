package org.kendar.cqrses.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;

/**
 * Idempotent schema bootstrap for the {@code kf-core-db} stores. The table shapes are identical on
 * both backends (UUIDs are {@code BINARY(16)}, instants are {@code BIGINT} epoch-millis, payloads are
 * {@code LONGBLOB}, {@code saga_correlation} uses {@code corr_value} because {@code VALUE} is a
 * reserved word in H2). Every statement is a {@code CREATE TABLE IF NOT EXISTS} so a re-init is a
 * no-op.
 * <p>
 * The one place the dialects genuinely diverge is <b>secondary index creation</b>, so the
 * initializer detects the backend from the JDBC metadata at runtime and runs the matching DDL:
 * <ul>
 *   <li><b>H2</b> ({@link #initializeH2()}) — standalone {@code CREATE INDEX IF NOT EXISTS}, which
 *       H2 supports.</li>
 *   <li><b>MySQL</b> ({@link #initializeMysql()}) — indexes declared <b>inline</b> in the
 *       {@code CREATE TABLE} ({@code INDEX name (cols)}). MySQL accepts {@code IF NOT EXISTS} on
 *       {@code CREATE TABLE} but <b>not</b> on {@code CREATE INDEX} (it is a parse error), so an
 *       inline index rides the table's own {@code IF NOT EXISTS} and stays idempotent.</li>
 * </ul>
 * H2 reports its product name as {@code H2} even in {@code MODE=MySQL}, so the detection cleanly
 * separates the embedded test/single-node store from a real MySQL server.
 * <p>
 * <b>Indexed-column width is constrained by real MySQL, not H2.</b> InnoDB caps an index key at
 * 3072 bytes, and the MySQL container is {@code utf8mb4} (4 bytes/char), so any composite key over
 * 768 chars is rejected — a limit H2's {@code MODE=MySQL} does not enforce, so it only surfaces on a
 * real server. That is why the saga {@code type} and {@code corr_value} columns are
 * {@code VARCHAR(255)}: the {@code saga_correlation} PK {@code (type, corr_value)} would otherwise be
 * {@code (512+512)×4 = 4096} bytes; at 255 each it is a comfortable {@code (255+255)×4 = 2040}. Keep
 * indexed {@code VARCHAR} widths small enough that every PK/UNIQUE/INDEX stays under 3072 bytes.
 */
public class SchemaInitializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaInitializer.class.getName());

    /** H2 flavour: tables plus standalone {@code CREATE INDEX IF NOT EXISTS}. */
    private static final List<String> DDL_H2 = List.of(
            """
            CREATE TABLE IF NOT EXISTS event_entry (
                id               BINARY(16)   NOT NULL PRIMARY KEY,
                aggregate_id     BINARY(16)   NOT NULL,
                processing_group VARCHAR(255) NOT NULL,
                segment          INT          NOT NULL,
                segment_seq      BIGINT       NOT NULL,
                event_type       VARCHAR(255) NOT NULL,
                sequence         BIGINT       NOT NULL,
                payload          LONGBLOB     NOT NULL,
                created_at       BIGINT       NOT NULL,
                UNIQUE (aggregate_id, sequence),
                UNIQUE (segment, segment_seq)
            )""",
            "CREATE INDEX IF NOT EXISTS idx_event_poll ON event_entry (processing_group, segment, sequence)",
            // projection poll: one segment, exact order by the gap-free tail position
            "CREATE INDEX IF NOT EXISTS idx_event_tail_seg ON event_entry (segment, segment_seq)",
            // saga per-source poll: one source segment, type-filtered, exact order
            "CREATE INDEX IF NOT EXISTS idx_event_tail_type ON event_entry (segment, event_type, segment_seq)",

            // One row per segment holding the next gap-free segment_seq to assign.
            // Incremented in the SAME transaction as the event insert (SELECT ... FOR
            // UPDATE) so an OCC rollback rolls the counter back too — no hole.
            """
            CREATE TABLE IF NOT EXISTS segment_counter (
                segment  INT    NOT NULL PRIMARY KEY,
                next_seq BIGINT NOT NULL
            )""",

            // Per-(group, owned-segment, source-segment) high-water-mark. Projections
            // write one row (source_segment == segment); sagas write SEGMENTS rows per
            // owned segment (one per merged source stream). segment_seq is gap-free so
            // last_seq is a clean high-water-mark — no gap tracking needed.
            """
            CREATE TABLE IF NOT EXISTS processor_checkpoint (
                processing_group VARCHAR(255) NOT NULL,
                segment          INT          NOT NULL,
                source_segment   INT          NOT NULL,
                last_seq         BIGINT       NOT NULL,
                updated_at       BIGINT       NOT NULL,
                PRIMARY KEY (processing_group, segment, source_segment)
            )""",

            """
            CREATE TABLE IF NOT EXISTS snapshot_entry (
                aggregate_id     BINARY(16)   NOT NULL PRIMARY KEY,
                processing_group VARCHAR(255) NOT NULL,
                segment          INT          NOT NULL,
                sequence         INT          NOT NULL,
                payload          LONGBLOB     NOT NULL,
                schema_version   BIGINT       NOT NULL DEFAULT 1,
                snapshot_type    VARCHAR(255),
                updated_at       BIGINT       NOT NULL
            )""",

            """
            CREATE TABLE IF NOT EXISTS saga_instance (
                saga_id        VARCHAR(255) NOT NULL,
                type           VARCHAR(255) NOT NULL,
                segment        INT          NOT NULL,
                correlation_id VARCHAR(512),
                content        LONGBLOB     NOT NULL,
                PRIMARY KEY (type, saga_id)
            )""",
            "CREATE INDEX IF NOT EXISTS idx_saga_segment ON saga_instance (segment)",
            "CREATE INDEX IF NOT EXISTS idx_saga_id ON saga_instance (saga_id)",

            """
            CREATE TABLE IF NOT EXISTS saga_correlation (
                type       VARCHAR(255) NOT NULL,
                corr_value VARCHAR(255) NOT NULL,
                saga_id    VARCHAR(255) NOT NULL,
                segment    INT          NOT NULL,
                PRIMARY KEY (type, corr_value)
            )""",
            "CREATE INDEX IF NOT EXISTS idx_corr_saga ON saga_correlation (type, saga_id)",

            """
            CREATE TABLE IF NOT EXISTS dlq_item (
                id                       BINARY(16)   NOT NULL PRIMARY KEY,
                sequence_id              VARCHAR(255) NOT NULL,
                ordinal                  BIGINT       NOT NULL,
                processing_group         VARCHAR(255),
                event_type               VARCHAR(255),
                aggregate_id             BINARY(16),
                serialized_event         LONGBLOB,
                processing_context       LONGBLOB,
                status                   VARCHAR(32)  NOT NULL,
                retry_count              INT          NOT NULL,
                error_message            TEXT,
                error_class              VARCHAR(512),
                stack_trace              TEXT,
                failed_at                BIGINT,
                last_retry_error_message TEXT,
                last_retry_error_class   VARCHAR(512),
                last_retry_stack_trace   TEXT,
                last_retry_at            BIGINT
            )""",
            "CREATE INDEX IF NOT EXISTS idx_dlq_seq ON dlq_item (sequence_id, ordinal)",

            // Segment-owned workload: only the segment's owner polls its rows, so there is
            // no cross-JVM OCC (no PICKED status / picked_by / version). The row is deleted
            // on success; a crash mid-fire leaves it due for the next owner to re-fire.
            // attempts counts throws for the backoff + cap-then-drop policy.
            """
            CREATE TABLE IF NOT EXISTS scheduled_task (
                id             BINARY(16)   NOT NULL PRIMARY KEY,
                task_name      VARCHAR(255) NOT NULL,
                segment        INT          NOT NULL,
                execution_time BIGINT       NOT NULL,
                params_json    LONGBLOB,
                attempts       INT          NOT NULL
            )""",
            "CREATE INDEX IF NOT EXISTS idx_sched_due ON scheduled_task (segment, execution_time)"
    );

    /**
     * MySQL flavour: identical tables, but indexes are declared inline (MySQL rejects
     * {@code CREATE INDEX IF NOT EXISTS}). {@code event_entry}'s {@code UNIQUE (segment, segment_seq)}
     * already serves the projection tail poll, so no separate {@code idx_event_tail_seg} is added.
     */
    private static final List<String> DDL_MYSQL = List.of(
            """
            CREATE TABLE IF NOT EXISTS event_entry (
                id               BINARY(16)   NOT NULL PRIMARY KEY,
                aggregate_id     BINARY(16)   NOT NULL,
                processing_group VARCHAR(255) NOT NULL,
                segment          INT          NOT NULL,
                segment_seq      BIGINT       NOT NULL,
                event_type       VARCHAR(255) NOT NULL,
                sequence         BIGINT       NOT NULL,
                payload          LONGBLOB     NOT NULL,
                created_at       BIGINT       NOT NULL,
                UNIQUE (aggregate_id, sequence),
                UNIQUE (segment, segment_seq),
                INDEX idx_event_poll (processing_group, segment, sequence),
                INDEX idx_event_tail_type (segment, event_type, segment_seq)
            )""",

            """
            CREATE TABLE IF NOT EXISTS segment_counter (
                segment  INT    NOT NULL PRIMARY KEY,
                next_seq BIGINT NOT NULL
            )""",

            """
            CREATE TABLE IF NOT EXISTS processor_checkpoint (
                processing_group VARCHAR(255) NOT NULL,
                segment          INT          NOT NULL,
                source_segment   INT          NOT NULL,
                last_seq         BIGINT       NOT NULL,
                updated_at       BIGINT       NOT NULL,
                PRIMARY KEY (processing_group, segment, source_segment)
            )""",

            """
            CREATE TABLE IF NOT EXISTS snapshot_entry (
                aggregate_id     BINARY(16)   NOT NULL PRIMARY KEY,
                processing_group VARCHAR(255) NOT NULL,
                segment          INT          NOT NULL,
                sequence         INT          NOT NULL,
                payload          LONGBLOB     NOT NULL,
                schema_version   BIGINT       NOT NULL DEFAULT 1,
                snapshot_type    VARCHAR(255),
                updated_at       BIGINT       NOT NULL
            )""",

            """
            CREATE TABLE IF NOT EXISTS saga_instance (
                saga_id        VARCHAR(255) NOT NULL,
                type           VARCHAR(255) NOT NULL,
                segment        INT          NOT NULL,
                correlation_id VARCHAR(512),
                content        LONGBLOB     NOT NULL,
                PRIMARY KEY (type, saga_id),
                INDEX idx_saga_segment (segment),
                INDEX idx_saga_id (saga_id)
            )""",

            """
            CREATE TABLE IF NOT EXISTS saga_correlation (
                type       VARCHAR(255) NOT NULL,
                corr_value VARCHAR(255) NOT NULL,
                saga_id    VARCHAR(255) NOT NULL,
                segment    INT          NOT NULL,
                PRIMARY KEY (type, corr_value),
                INDEX idx_corr_saga (type, saga_id)
            )""",

            """
            CREATE TABLE IF NOT EXISTS dlq_item (
                id                       BINARY(16)   NOT NULL PRIMARY KEY,
                sequence_id              VARCHAR(255) NOT NULL,
                ordinal                  BIGINT       NOT NULL,
                processing_group         VARCHAR(255),
                event_type               VARCHAR(255),
                aggregate_id             BINARY(16),
                serialized_event         LONGBLOB,
                processing_context       LONGBLOB,
                status                   VARCHAR(32)  NOT NULL,
                retry_count              INT          NOT NULL,
                error_message            TEXT,
                error_class              VARCHAR(512),
                stack_trace              TEXT,
                failed_at                BIGINT,
                last_retry_error_message TEXT,
                last_retry_error_class   VARCHAR(512),
                last_retry_stack_trace   TEXT,
                last_retry_at            BIGINT,
                INDEX idx_dlq_seq (sequence_id, ordinal)
            )""",

            """
            CREATE TABLE IF NOT EXISTS scheduled_task (
                id             BINARY(16)   NOT NULL PRIMARY KEY,
                task_name      VARCHAR(255) NOT NULL,
                segment        INT          NOT NULL,
                execution_time BIGINT       NOT NULL,
                params_json    LONGBLOB,
                attempts       INT          NOT NULL,
                INDEX idx_sched_due (segment, execution_time)
            )"""
    );

    private final Db db;

    public SchemaInitializer(Db db) {
        this.db = db;
    }

    /**
     * Creates every table and index if absent, using the backend's own index syntax. Idempotent —
     * safe to call on each boot.
     */
    public void initialize() {
        if (isMysql()) {
            initializeMysql();
        } else {
            initializeH2();
        }
    }

    private void initializeH2() {
        run(DDL_H2);
        LOGGER.trace("kf-core-db schema initialized (H2)");
    }

    private void initializeMysql() {
        run(DDL_MYSQL);
        LOGGER.trace("kf-core-db schema initialized (MySQL)");
    }

    private void run(List<String> ddl) {
        for (String stmt : ddl) {
            db.execute(stmt);
        }
    }

    /** True on a real MySQL server. H2 reports {@code H2} even in {@code MODE=MySQL}. */
    private boolean isMysql() {
        try (Connection c = db.connection()) {
            String product = c.getMetaData().getDatabaseProductName();
            return product != null && product.toLowerCase(Locale.ROOT).contains("mysql");
        } catch (SQLException e) {
            throw new DbException(e);
        }
    }
}
