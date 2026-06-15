package org.kendar.cqrses.db;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Coverage for the column-keyed {@link InsertBuilder}: ordered column/value
 * pairing, the MySQL-mode {@code INSERT IGNORE} duplicate-key drop, and the
 * empty-columns guard.
 */
class InsertBuilderTest {

    private static final AtomicInteger DB_SEQ = new AtomicInteger();

    private Db db;

    @BeforeEach
    void setUp() {
        ConnectionStorage.unbind();
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:insbuilder_" + DB_SEQ.incrementAndGet() + ";MODE=MySQL;DB_CLOSE_DELAY=-1");
        db = new DefaultDb(ds);
        db.execute("CREATE TABLE item (id INT PRIMARY KEY, label VARCHAR(64), qty INT)");
    }

    @Test
    void insertsRowWithColumnsInSetOrder() {
        int affected = db.insertInto("item")
                .set("id", 1)
                .set("label", "widget")
                .set("qty", 7)
                .execute();
        assertEquals(1, affected);

        assertEquals("widget", db.queryForObject("SELECT label FROM item WHERE id = ?", String.class, 1));
        assertEquals(7, db.queryForObject("SELECT qty FROM item WHERE id = ?", Integer.class, 1));
    }

    @Test
    void plainInsertThrowsOnDuplicateKey() {
        db.insertInto("item").set("id", 1).set("label", "first").execute();
        assertThrows(DbException.class,
                () -> db.insertInto("item").set("id", 1).set("label", "dup").execute());
    }

    @Test
    void ignoreSilentlyDropsDuplicateKey() {
        assertEquals(1, db.insertInto("item").set("id", 1).set("label", "first").execute());

        int affected = db.insertInto("item").ignore().set("id", 1).set("label", "second").execute();
        assertEquals(0, affected);

        // original row untouched
        assertEquals("first", db.queryForObject("SELECT label FROM item WHERE id = ?", String.class, 1));
        assertEquals(1L, db.queryForObject("SELECT COUNT(*) FROM item", Long.class));
    }

    @Test
    void executeWithoutColumnsThrowsIllegalState() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> db.insertInto("item").execute());
        assertEquals("no columns set", ex.getMessage());
    }
}
