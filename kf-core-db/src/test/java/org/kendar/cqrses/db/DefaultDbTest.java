package org.kendar.cqrses.db;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Happy-path coverage for {@link DefaultDb} against a real in-memory H2 in
 * {@code MODE=MySQL}: every {@link Db} method (execute, update, batchUpdate, the
 * three query overloads, queryForList) plus the {@code connection()} seam. The
 * error paths live in {@link DbErrorPathTest}.
 */
class DefaultDbTest {

    private static final AtomicInteger DB_SEQ = new AtomicInteger();

    private Db db;

    @BeforeEach
    void setUp() {
        // connection() binds a connection to the thread for the duration of a path and DefaultDb
        // never closes it (that lifecycle belongs to the path boundary). Each test method is one
        // path running on the shared JUnit thread, so drop any connection a prior method left bound
        // before opening this method's fresh datasource — otherwise CREATE TABLE runs against the
        // stale DB and fails with "Table PERSON already exists".
        ConnectionStorage.unbind();
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:defaultdb_" + DB_SEQ.incrementAndGet() + ";MODE=MySQL;DB_CLOSE_DELAY=-1");
        db = new DefaultDb(ds);
        db.execute("CREATE TABLE person (id INT PRIMARY KEY, name VARCHAR(64), age INT)");
    }

    @Test
    void executeRunsDdlAndUpdateReturnsAffectedRowCount() {
        assertEquals(1, db.update("INSERT INTO person(id, name, age) VALUES (?, ?, ?)", 1, "alice", 30));
        assertEquals(1, db.update("INSERT INTO person(id, name, age) VALUES (?, ?, ?)", 2, "bob", 40));
        assertEquals(2, db.update("UPDATE person SET age = age + 1 WHERE age >= ?", 30));
        assertEquals(1, db.update("DELETE FROM person WHERE id = ?", 1));
    }

    @Test
    void batchUpdateAppliesEveryRow() {
        int[] counts = db.batchUpdate(
                "INSERT INTO person(id, name, age) VALUES (?, ?, ?)",
                List.of(
                        new Object[]{1, "alice", 30},
                        new Object[]{2, "bob", 40},
                        new Object[]{3, "carol", 50}));
        assertEquals(3, counts.length);
        for (int c : counts) assertEquals(1, c);
        assertEquals(3L, db.queryForObject("SELECT COUNT(*) FROM person", Long.class));
    }

    @Test
    void queryWithRowMapperMapsEveryRowAndIncrementsRowNum() {
        db.update("INSERT INTO person(id, name, age) VALUES (?, ?, ?)", 1, "alice", 30);
        db.update("INSERT INTO person(id, name, age) VALUES (?, ?, ?)", 2, "bob", 40);

        List<String> labelled = db.query(
                "SELECT name FROM person ORDER BY id",
                (rs, rowNum) -> rowNum + ":" + rs.getString("name"));
        assertEquals(List.of("0:alice", "1:bob"), labelled);
    }

    @Test
    void queryForObjectWithRowMapperReturnsSingleRowOrNull() {
        db.update("INSERT INTO person(id, name, age) VALUES (?, ?, ?)", 1, "alice", 30);

        String name = db.queryForObject(
                "SELECT name FROM person WHERE id = ?",
                (rs, rowNum) -> rs.getString("name"), 1);
        assertEquals("alice", name);

        assertNull(db.queryForObject(
                "SELECT name FROM person WHERE id = ?",
                (rs, rowNum) -> rs.getString("name"), 999));
    }

    @Test
    void queryForObjectWithTypeReturnsScalarOrNull() {
        db.update("INSERT INTO person(id, name, age) VALUES (?, ?, ?)", 1, "alice", 30);

        assertEquals("alice", db.queryForObject("SELECT name FROM person WHERE id = ?", String.class, 1));
        assertNull(db.queryForObject("SELECT name FROM person WHERE id = ?", String.class, 999));
    }

    @Test
    void queryForListReturnsAllScalarsInOrder() {
        db.update("INSERT INTO person(id, name, age) VALUES (?, ?, ?)", 1, "alice", 30);
        db.update("INSERT INTO person(id, name, age) VALUES (?, ?, ?)", 2, "bob", 40);
        db.update("INSERT INTO person(id, name, age) VALUES (?, ?, ?)", 3, "carol", 50);

        List<String> names = db.queryForList("SELECT name FROM person ORDER BY id", String.class);
        assertEquals(List.of("alice", "bob", "carol"), names);

        assertTrue(db.queryForList("SELECT name FROM person WHERE id = ?", String.class, 999).isEmpty());
    }

    @Test
    void connectionSeamReturnsAUsableConnection() throws SQLException {
        try (Connection conn = db.connection()) {
            assertFalse(conn.isClosed());
            assertTrue(conn.isValid(1));
        }
    }
}
