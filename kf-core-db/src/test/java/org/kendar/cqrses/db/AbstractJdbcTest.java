package org.kendar.cqrses.db;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.serialization.JacksonMessageSerializer;
import org.kendar.cqrses.serialization.MessageSerializer;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base for the JDBC store/scheduler tests: a fresh in-memory H2 database in
 * {@code MODE=MySQL} per test method, with the {@code kf-core-db} schema applied
 * and a {@link JacksonMessageSerializer} registered in {@code GlobalRegistry}.
 * Mirrors the {@code GlobalRegistry.clear()} + register dance the in-memory
 * store tests use.
 */
public abstract class AbstractJdbcTest {

    private static final AtomicInteger DB_SEQ = new AtomicInteger();

    protected Db db;
    protected MessageSerializer<?, ?> serializer;

    @BeforeEach
    void baseSetUp() {
        GlobalRegistry.clear();
        serializer = new JacksonMessageSerializer();
        GlobalRegistry.register(MessageSerializer.class, serializer);

        // connection() binds a connection to the thread for the duration of a path and DefaultDb
        // never closes it (that lifecycle belongs to the path boundary). Drop any connection a
        // prior test method left bound to the shared JUnit thread, otherwise connection() reuses it
        // and this method runs against the previous method's DB instead of its fresh datasource —
        // SchemaInitializer's CREATE TABLE IF NOT EXISTS hides it, so isolation is lost silently.
        ConnectionStorage.unbind();
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:kfdb_" + DB_SEQ.incrementAndGet() + ";MODE=MySQL;DB_CLOSE_DELAY=-1");
        db = new DefaultDb(ds);
        new SchemaInitializer(db).initialize();
    }

    @AfterEach
    void baseTearDown() {
        GlobalRegistry.clear();
    }
}
