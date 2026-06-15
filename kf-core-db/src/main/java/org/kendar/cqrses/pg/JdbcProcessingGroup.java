package org.kendar.cqrses.pg;

import org.kendar.cqrses.bus.Bus;
import org.kendar.cqrses.db.ConnectionStorage;
import org.kendar.cqrses.db.DefaultDb;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.dlq.DlqStore;
import org.kendar.cqrses.observability.AppendPhase;
import org.kendar.cqrses.observability.Observability;
import org.kendar.cqrses.serialization.MessageSerializer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class JdbcProcessingGroup extends ProcessingGroup{
    /**
     * True when this boundary bound the thread's connection in {@link #transactionStart()} (the usual
     * case) and is therefore responsible for closing + unbinding it on end/rollback. False when an
     * outer caller had already bound one — then we only restore its autocommit and leave it open.
     */
    private boolean ownsConnection;

    public JdbcProcessingGroup(String name, Bus bus, MessageSerializer serializer, boolean commandSide, DlqStore dlqStore, Map<Class<?>, List<Bus.Registration>> consumer, Bus.ProcessingGroupPolicyConfig policy) {
        super(name, bus, serializer, commandSide, dlqStore, consumer, policy);
    }

    @Override
    protected void transactionRollback() {
        try {
            Connection conn = ConnectionStorage.get();
            conn.rollback();
            finishTransaction(conn);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            // The appended rows never became visible; drop any pending nudge.
            PumpNudger.onRollback();
        }
    }

    @Override
    protected void transactionStart() {
        var ddb = GlobalRegistry.get(DefaultDb.class);

        try {
            if(ConnectionStorage.get()==null){
                ConnectionStorage.bind(ddb.connection());
                ownsConnection = true;
            }
            ConnectionStorage.get().setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void transactionEnd() {
        try {
            Connection conn = ConnectionStorage.get();
            long mark = System.nanoTime();
            conn.commit();
            Observability.get().onAppendPhase(AppendPhase.BOUNDARY_COMMIT, System.nanoTime() - mark);
            finishTransaction(conn);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        // Post-commit hook: fire the pump nudge deferred by a publish that ran
        // inside this boundary (PumpNudger.notifyAppend saw the bound connection).
        PumpNudger.afterCommit();
    }

    /**
     * Restore autocommit, then release the connection if we bound it. Leaving a connection bound
     * (autoCommit=false) latched to the thread leaks a pool slot and, on MySQL, pins a stale
     * REPEATABLE READ snapshot so a long-lived poll loop (the cluster pull pump) stops seeing
     * newly-committed rows. Closing returns it to the pool and clears the thread binding.
     */
    private void finishTransaction(Connection conn) throws SQLException {
        conn.setAutoCommit(true);
        if (ownsConnection) {
            ownsConnection = false;
            ConnectionStorage.unbind();
            conn.close();
        }
    }
}
