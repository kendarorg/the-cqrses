package org.kendar.cqrses.pg;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.db.ConnectionStorage;
import org.kendar.cqrses.observability.Observability;
import org.kendar.cqrses.observability.TestObservabilityAdapter;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PumpNudgerTest {

    private final AtomicInteger fired = new AtomicInteger();
    private final Runnable listener = fired::incrementAndGet;

    @AfterEach
    void tearDown() {
        PumpNudger.clear();
        PumpNudger.onRollback(); // drop any deferred flag left on this thread
        ConnectionStorage.unbind();
    }

    private Connection h2Connection() throws Exception {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:pumpnudger;DB_CLOSE_DELAY=-1");
        return ds.getConnection();
    }

    @Test
    void firesImmediatelyWithoutBoundConnection() {
        PumpNudger.register(listener);
        PumpNudger.notifyAppend();
        assertEquals(1, fired.get());
    }

    @Test
    void defersWhileTransactionBoundAndFiresAfterCommit() throws Exception {
        PumpNudger.register(listener);
        try (Connection conn = h2Connection()) {
            ConnectionStorage.bind(conn);
            try {
                PumpNudger.notifyAppend();
                assertEquals(0, fired.get(), "must not fire before the boundary commits");
            } finally {
                ConnectionStorage.unbind();
            }
        }
        PumpNudger.afterCommit();
        assertEquals(1, fired.get());
    }

    @Test
    void rollbackDiscardsDeferredNudge() throws Exception {
        PumpNudger.register(listener);
        try (Connection conn = h2Connection()) {
            ConnectionStorage.bind(conn);
            try {
                PumpNudger.notifyAppend();
            } finally {
                ConnectionStorage.unbind();
            }
        }
        PumpNudger.onRollback();
        PumpNudger.afterCommit();
        assertEquals(0, fired.get());
    }

    @Test
    void afterCommitWithoutPendingNudgeIsNoOp() {
        PumpNudger.register(listener);
        PumpNudger.afterCommit();
        assertEquals(0, fired.get());
    }

    @Test
    void multipleAppendsCollapseToDeferredFlagFiredOnce() throws Exception {
        PumpNudger.register(listener);
        try (Connection conn = h2Connection()) {
            ConnectionStorage.bind(conn);
            try {
                PumpNudger.notifyAppend();
                PumpNudger.notifyAppend();
                PumpNudger.notifyAppend();
            } finally {
                ConnectionStorage.unbind();
            }
        }
        PumpNudger.afterCommit();
        assertEquals(1, fired.get());
    }

    @Test
    void unregisterAndClearStopDelivery() {
        PumpNudger.register(listener);
        PumpNudger.unregister(listener);
        PumpNudger.notifyAppend();
        assertEquals(0, fired.get());

        PumpNudger.register(listener);
        PumpNudger.clear();
        PumpNudger.notifyAppend();
        assertEquals(0, fired.get());
    }

    @Test
    void nudgeCountersDistinguishImmediateFromDeferred() throws Exception {
        var immediate = new AtomicInteger();
        var deferred = new AtomicInteger();
        Observability.set(new TestObservabilityAdapter() {
            @Override
            public void onPumpNudge(boolean wasDeferred) {
                (wasDeferred ? deferred : immediate).incrementAndGet();
            }
        });
        try {
            PumpNudger.register(listener);
            PumpNudger.notifyAppend();
            assertEquals(1, immediate.get());
            assertEquals(0, deferred.get());

            try (Connection conn = h2Connection()) {
                ConnectionStorage.bind(conn);
                try {
                    PumpNudger.notifyAppend();
                } finally {
                    ConnectionStorage.unbind();
                }
            }
            PumpNudger.afterCommit();
            assertEquals(1, immediate.get());
            assertEquals(1, deferred.get());
        } finally {
            Observability.set(null);
        }
    }

    @Test
    void throwingListenerDoesNotBreakOthers() {
        PumpNudger.register(() -> {
            throw new IllegalStateException("boom");
        });
        PumpNudger.register(listener);
        PumpNudger.notifyAppend();
        assertEquals(1, fired.get());
    }
}
