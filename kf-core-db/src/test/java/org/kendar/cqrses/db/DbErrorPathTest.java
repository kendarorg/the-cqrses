package org.kendar.cqrses.db;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exercises the {@link DefaultDb#connection()} template-method seam from
 * CROSS_CUTTING.md's testing strategy: a subclass whose {@code connection()} throws
 * {@link SQLException} drives the error path with no Mockito and no
 * {@code --add-opens}. Every {@link Db} operation must surface a
 * {@link DbException}.
 */
class DbErrorPathTest {

    private static Db throwingDb() {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:err_unused;MODE=MySQL");
        return new DefaultDb(ds) {
            @Override
            public Connection connection() throws SQLException {
                throw new SQLException("connection refused (test)");
            }
        };
    }


    @Test
    void executePropagatesDbException() {
        Db db = throwingDb();
        DbException ex = assertThrows(DbException.class, () -> db.execute("CREATE TABLE x(a INT)"));
        assertInstanceOf(SQLException.class, ex.getCause());
    }

    @Test
    void updateQueryAndBuildersPropagateDbException() {
        Db db = throwingDb();
        assertThrows(DbException.class, () -> db.update("DELETE FROM x"));
        assertThrows(DbException.class, () -> db.queryForObject("SELECT 1", Integer.class));
        assertThrows(DbException.class, () -> db.queryForList("SELECT 1", Integer.class));
        assertThrows(DbException.class, () -> db.insertInto("x").set("a", 1).execute());
        assertThrows(DbException.class, () -> db.updateTable("x").set("a", 1).where("b", 2).execute());
    }

    @Test
    void happyPathDataSourceStillWorks() {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:err_ok;MODE=MySQL;DB_CLOSE_DELAY=-1");
        Db db = new DefaultDb(ds);
        assertDoesNotThrow(() -> db.execute("CREATE TABLE IF NOT EXISTS t(a INT)"));
        db.update("INSERT INTO t(a) VALUES (?)", 42);
        assertEquals(42, db.queryForObject("SELECT a FROM t", Integer.class));
    }
}
