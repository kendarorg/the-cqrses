package org.kendar.cqrses.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Thin Spring-free JDBC wrapper. Connection lifecycle is the implementation's
 * responsibility — see {@link DefaultDb}, which <b>binds one connection per thread</b>
 * (via {@link ConnectionStorage}) and reuses it across calls, leaving commit/close to
 * the owning transaction boundary; connection pooling is the DataSource's job.
 * <p>
 * Ported from {@code olds/kf-es-cluster} — the only piece of that tree carried
 * forward — and stripped of every PostgreSQL-ism for the H2 &cap; MySQL target.
 */
public interface Db {

    /**
     * DDL and other no-result statements.
     */
    void execute(String sql);

    /**
     * INSERT / UPDATE / DELETE. Returns update count.
     */
    int update(String sql, Object... args);

    /**
     * Batch INSERT / UPDATE. Returns per-row update counts.
     */
    int[] batchUpdate(String sql, List<Object[]> batches);

    /**
     * SELECT returning a list of mapped rows.
     */
    <T> List<T> query(String sql, RowMapper<T> mapper, Object... args);

    /**
     * SELECT returning a single mapped row, or null if no row found.
     */
    <T> T queryForObject(String sql, RowMapper<T> mapper, Object... args);

    /**
     * SELECT returning a single scalar value, or null if no row found.
     */
    <T> T queryForObject(String sql, Class<T> type, Object... args);

    /**
     * SELECT returning a list of scalar values.
     */
    <T> List<T> queryForList(String sql, Class<T> type, Object... args);

    /**
     * Column-keyed INSERT builder; see {@link InsertBuilder}.
     */
    InsertBuilder insertInto(String table);

    /**
     * Column-keyed UPDATE builder; see {@link UpdateBuilder}.
     */
    UpdateBuilder updateTable(String table);

    /**
     * Raw connection for multi-statement transactions. Overridable seam: tests
     * pass an anonymous subclass whose {@code connection()} throws to exercise
     * error paths without Mockito or {@code --add-opens}.
     */
    Connection connection() throws SQLException;
}
