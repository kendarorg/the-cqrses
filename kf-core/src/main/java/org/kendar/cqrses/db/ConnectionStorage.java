package org.kendar.cqrses.db;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Process-wide thread-bound carrier for a single JDBC {@link Connection}, used to simulate Spring's
 * {@code @Transactional} without dragging in Spring.
 * <p>
 * {@link DefaultDb} lazily binds <b>one connection per thread</b> here on first use and reuses it for
 * every later call on that thread — it is <b>not</b> opened-and-closed per call. A transaction
 * boundary {@link #bind(Connection) binds} a connection (with {@code autoCommit=false}) to make a
 * sequence of operations atomic the way {@code @Transactional} does, then commits (or rolls back) and
 * {@link #unbind() unbinds} when the unit of work finishes. <b>A thread that never goes through such a
 * boundary (e.g. a long-lived poll loop) keeps the same connection open indefinitely</b> — the owner
 * of that thread must commit/close it periodically or its read snapshot can go stale.
 * <p>
 * All members are {@code static}: the carrier is a single thread-local seam consulted from anywhere
 * (the {@code Db} and the transaction boundary) without having to thread an instance through. The
 * {@link ThreadLocal} it wraps is per-thread, so concurrent lane threads each carry their own
 * connection independently.
 */
public final class ConnectionStorage {

    private static final ThreadLocal<Connection> CURRENT = new ThreadLocal<>();

    private ConnectionStorage() {
    }

    /**
     * Binds {@code connection} to the current thread for the duration of a transaction.
     *
     * @throws IllegalStateException if a connection is already bound to this thread — the caller is
     *                               opening a nested transaction without first unbinding the outer
     *                               one, which this carrier does not model (no propagation).
     */
    public static void bind(Connection connection) {
        if (connection == null) {
            throw new IllegalArgumentException("connection must not be null");
        }
        try {
            if (CURRENT.get() != null && !CURRENT.get().isClosed()) {
                throw new IllegalStateException("a connection is already bound to this thread");
            }
        } catch (SQLException e) {

        }
        CURRENT.set(connection);
    }

    /**
     * The connection bound to the current thread, or {@code null} if none — i.e. the caller is not
     * inside a simulated transaction and should open (and close) its own connection per call.
     */
    public static Connection get() {
        return CURRENT.get();
    }

    /**
     * Whether a connection is bound to the current thread, i.e. a simulated transaction is active.
     */
    public static boolean isBound() {
        return CURRENT.get() != null;
    }

    /**
     * Removes the binding for the current thread. Does <b>not</b> commit, roll back, or close the
     * connection — lifecycle is the transaction boundary's responsibility. Safe to call when nothing
     * is bound. Always call this (typically in a {@code finally}) so the thread, which may be reused
     * from a pool, does not leak the connection into the next unit of work.
     */
    public static void unbind() {
        CURRENT.remove();
    }
}
