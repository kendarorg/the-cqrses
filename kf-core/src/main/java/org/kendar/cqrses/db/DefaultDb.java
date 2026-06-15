package org.kendar.cqrses.db;

import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.observability.Observability;
import org.kendar.cqrses.observability.SqlCategory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Default {@link Db} implementation over a supplied {@link DataSource}.
 * <p>
 * <b>Connection lifecycle.</b> When a {@link ConnectionStorage}-aware transaction boundary (e.g.
 * {@code JdbcProcessingGroup}) has {@link ConnectionStorage#bind bound} a connection to the current
 * thread, every call reuses that bound connection so a sequence of statements runs in one
 * transaction (Spring's {@code @Transactional} without Spring); the boundary owns commit/close/unbind.
 * <p>
 * Otherwise — the common case, and <b>every</b> call made outside a transaction — each call opens a
 * fresh connection and <b>closes it before returning</b>. This is critical for long-lived loop
 * threads (the cluster pull pump, the cluster worker/leader/heartbeat): a connection retained across
 * a poll loop would both leak a pool slot and, on MySQL, pin a stale REPEATABLE READ snapshot. See
 * {@code ConnectionStorage}.
 * <p>
 * Subclassable so tests can override {@link #connection()} to return an in-memory connection or throw.
 */
public class DefaultDb implements Db {

    /**
     * SQL trace logger. Enable it (e.g. {@code <logger name="org.kendar.cqrses.db" level="DEBUG"/>})
     * to log every statement with its {@code ?} placeholders interpolated — see {@link SqlApproximator}.
     * Guarded by {@link Logger#isDebugEnabled()} so disabled logging costs nothing.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDb.class);

    private final DataSource ds;

    public DefaultDb(DataSource ds) {

        this.ds = ds;
        if(GlobalRegistry.get(DefaultDb.class)==null) {
            GlobalRegistry.register(DefaultDb.class, this);
        }
    }

    private static void bind(PreparedStatement ps, Object[] args) throws SQLException {
        for (int i = 0; i < args.length; i++) {
            ps.setObject(i + 1, args[i]);
        }
    }

    /** Log the approximate (parameter-interpolated) SQL when debug logging is enabled. */
    private static void logSql(String sql, Object... args) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(SqlApproximator.approximate(sql, args));
        }
    }

    /**
     * Release a connection obtained from {@link #connection()} <b>only if it is ad-hoc</b> (not the
     * thread's transaction-bound connection — that one is the boundary's to close). Quiet: a failure
     * to close must not mask the operation's own outcome.
     */
    private static void release(Connection conn) {
        if (conn != null && conn != ConnectionStorage.get()) {
            try {
                conn.close();
            } catch (SQLException ignored) {
            }
        }
    }

    @Override
    public void execute(String sql) {
        inConnection(sql, conn -> {
            try (Statement stmt = conn.createStatement()) {
                logSql(sql);
                stmt.execute(sql);
            }
            return null;
        });
    }

    @Override
    public int update(String sql, Object... args) {
        return inConnection(sql, conn -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                logSql(sql, args);
                bind(ps, args);
                return ps.executeUpdate();
            }
        });
    }

    @Override
    public int[] batchUpdate(String sql, List<Object[]> batches) {
        return inConnection(sql, conn -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (Object[] row : batches) {
                    logSql(sql, row);
                    bind(ps, row);
                    ps.addBatch();
                }
                return ps.executeBatch();
            }
        });
    }

    @Override
    public <T> List<T> query(String sql, RowMapper<T> mapper, Object... args) {
        return inConnection(sql, conn -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                logSql(sql, args);
                bind(ps, args);
                try (ResultSet rs = ps.executeQuery()) {
                    List<T> result = new ArrayList<>();
                    int rowNum = 0;
                    while (rs.next()) {
                        result.add(mapper.mapRow(rs, rowNum++));
                    }
                    return result;
                }
            }
        });
    }

    @Override
    public <T> T queryForObject(String sql, RowMapper<T> mapper, Object... args) {
        return inConnection(sql, conn -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                logSql(sql, args);
                bind(ps, args);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) return null;
                    return mapper.mapRow(rs, 0);
                }
            }
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T queryForObject(String sql, Class<T> type, Object... args) {
        return inConnection(sql, conn -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                logSql(sql, args);
                bind(ps, args);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) return null;
                    return (T) rs.getObject(1);
                }
            }
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> List<T> queryForList(String sql, Class<T> type, Object... args) {
        return inConnection(sql, conn -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                logSql(sql, args);
                bind(ps, args);
                try (ResultSet rs = ps.executeQuery()) {
                    List<T> result = new ArrayList<>();
                    while (rs.next()) {
                        result.add((T) rs.getObject(1));
                    }
                    return result;
                }
            }
        });
    }

    /**
     * One operation against a connection, timed and reported by SQL category.
     */
    @FunctionalInterface
    private interface ConnOp<T> {
        T run(Connection conn) throws SQLException;
    }

    /**
     * Acquire a connection (bound or ad-hoc), run {@code op}, release the ad-hoc
     * one, and report the elapsed time to {@link Observability} keyed by the
     * statement's {@link SqlCategory}. Any {@link SQLException} — from connection
     * acquisition or the statement itself — is wrapped as {@link DbException};
     * the timing records {@code ok=false} on that path.
     */
    private <T> T inConnection(String sql, ConnOp<T> op) {
        long t0 = System.nanoTime();
        boolean ok = false;
        Connection conn = null;
        try {
            conn = connection();
            T result = op.run(conn);
            ok = true;
            return result;
        } catch (SQLException e) {
            throw new DbException(e);
        } finally {
            Observability.get().onSqlExecuted(SqlCategory.of(sql), System.nanoTime() - t0, ok);
            release(conn);
        }
    }

    @Override
    public InsertBuilder insertInto(String table) {
        return new InsertBuilder(this, table);
    }

    @Override
    public UpdateBuilder updateTable(String table) {
        return new UpdateBuilder(this, table);
    }

    /**
     * The connection to run a statement on: the thread's transaction-bound connection when a
     * boundary has bound one, otherwise a <b>fresh</b> connection the caller must close (the
     * {@code Db} methods do, via {@link #release}; raw callers — {@code appendForAggregate}, the
     * transaction boundary's {@code bind(...)}, schema init — own it explicitly). Does <b>not</b>
     * auto-bind: an ad-hoc call must not leave a connection latched to the thread.
     */
    @Override
    public Connection connection() throws SQLException {
        Connection bound = ConnectionStorage.get();
        if (bound != null && !bound.isClosed()) {
            return bound;
        }
        return ds.getConnection();
    }
}
