package org.kendar.cqrses.db;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Coverage for the column-keyed {@link UpdateBuilder}: SET/WHERE pairing, the
 * multi-column {@code AND}-joined WHERE, and the two guards (no SET, no WHERE)
 * that keep an accidental table-wide UPDATE from executing.
 */
class UpdateBuilderTest {

    private static final AtomicInteger DB_SEQ = new AtomicInteger();

    private Db db;

    @BeforeEach
    void setUp() {
        ConnectionStorage.unbind();
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:updbuilder_" + DB_SEQ.incrementAndGet() + ";MODE=MySQL;DB_CLOSE_DELAY=-1");
        db = new DefaultDb(ds);
        db.execute("CREATE TABLE account (id INT PRIMARY KEY, owner VARCHAR(64), balance INT, status VARCHAR(16))");
        db.update("INSERT INTO account(id, owner, balance, status) VALUES (?, ?, ?, ?)", 1, "alice", 100, "open");
        db.update("INSERT INTO account(id, owner, balance, status) VALUES (?, ?, ?, ?)", 2, "bob", 200, "open");
    }

    @Test
    void updatesMatchingRowAndReturnsCount() {
        int affected = db.updateTable("account")
                .set("balance", 150)
                .set("status", "frozen")
                .where("id", 1)
                .execute();
        assertEquals(1, affected);

        assertEquals(150, db.queryForObject("SELECT balance FROM account WHERE id = ?", Integer.class, 1));
        assertEquals("frozen", db.queryForObject("SELECT status FROM account WHERE id = ?", String.class, 1));
        // row 2 untouched
        assertEquals(200, db.queryForObject("SELECT balance FROM account WHERE id = ?", Integer.class, 2));
    }

    @Test
    void multiColumnWhereIsAndJoined() {
        int affected = db.updateTable("account")
                .set("status", "closed")
                .where("owner", "alice")
                .where("status", "open")
                .execute();
        assertEquals(1, affected);
        assertEquals("closed", db.queryForObject("SELECT status FROM account WHERE id = ?", String.class, 1));

        // a non-matching AND combination touches nothing
        int none = db.updateTable("account")
                .set("status", "closed")
                .where("owner", "bob")
                .where("status", "closed")
                .execute();
        assertEquals(0, none);
    }

    @Test
    void executeWithoutSetThrowsIllegalState() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> db.updateTable("account").where("id", 1).execute());
        assertEquals("no columns to set", ex.getMessage());
    }

    @Test
    void executeWithoutWhereIsRefused() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> db.updateTable("account").set("status", "closed").execute());
        assertEquals("refusing UPDATE without WHERE", ex.getMessage());

        // guard fired before any SQL ran — both rows still open
        assertEquals(2L, db.queryForObject("SELECT COUNT(*) FROM account WHERE status = ?", Long.class, "open"));
    }
}
