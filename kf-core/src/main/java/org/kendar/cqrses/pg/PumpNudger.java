package org.kendar.cqrses.pg;

import org.kendar.cqrses.db.ConnectionStorage;
import org.kendar.cqrses.observability.Observability;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 * In-JVM wakeup channel from the append side to the pull pumps, so a locally
 * appended event is dispatched with ~zero latency instead of waiting for the
 * pumps' periodic backstop poll (cross-node events still arrive via that poll —
 * a remote JVM cannot nudge this one).
 *
 * <p>Static holder, mirroring {@link org.kendar.cqrses.observability.Observability}:
 * pull-pump workers {@link #register} a listener (a cheap, never-throwing wakeup
 * release) at start and {@link #unregister} on stop; the publish path calls
 * {@link #notifyAppend()} after handing the event to the store.
 *
 * <p><b>Commit visibility.</b> A nudge must not fire before the appended rows are
 * visible to the pumps, or the woken drain reads nothing and goes back to sleep.
 * Two shapes exist on the command side:
 * <ul>
 *   <li><b>sendSync</b> — the store appends on an ad-hoc connection and commits
 *       before the publish reaches this class; no connection is bound to the
 *       thread, so {@link #notifyAppend()} fires immediately (already
 *       post-commit).</li>
 *   <li><b>async boundary</b> — {@code JdbcProcessingGroup} binds a thread
 *       connection ({@link ConnectionStorage}) and commits only in
 *       {@code transactionEnd()} <i>after</i> the publish. Here
 *       {@link #notifyAppend()} only sets a per-thread deferred flag; the
 *       boundary calls {@link #afterCommit()} once the commit succeeded (or
 *       {@link #onRollback()} to discard the pending nudge).</li>
 * </ul>
 * The in-memory backend never binds a connection and its appends are immediately
 * visible, so the immediate-fire path is always correct there. With no listeners
 * registered (push mode, command side) every call is a no-op.
 */
public final class PumpNudger {

    private static final CopyOnWriteArrayList<Runnable> LISTENERS = new CopyOnWriteArrayList<>();
    /** Set while a bound transaction is in flight and an append happened inside it. */
    private static final ThreadLocal<Boolean> DEFERRED = new ThreadLocal<>();

    private PumpNudger() {
    }

    /** Registers a pump wakeup. The listener must be cheap and must not throw. */
    public static void register(Runnable listener) {
        LISTENERS.addIfAbsent(listener);
    }

    public static void unregister(Runnable listener) {
        LISTENERS.remove(listener);
    }

    /** Drops every listener — framework stop / test teardown. */
    public static void clear() {
        LISTENERS.clear();
    }

    /**
     * An event was appended and published on this thread. Fires the listeners
     * immediately unless a transaction is bound to the thread (append not yet
     * committed) — then the nudge is deferred to {@link #afterCommit()}.
     */
    public static void notifyAppend() {
        if (LISTENERS.isEmpty()) {
            return;
        }
        if (ConnectionStorage.isBound()) {
            DEFERRED.set(Boolean.TRUE);
            return;
        }
        Observability.get().onPumpNudge(false);
        fire();
    }

    /** The boundary's commit succeeded: fire any nudge deferred on this thread. */
    public static void afterCommit() {
        if (DEFERRED.get() != null) {
            DEFERRED.remove();
            Observability.get().onPumpNudge(true);
            fire();
        }
    }

    /** The boundary rolled back: the appended rows never became visible. */
    public static void onRollback() {
        DEFERRED.remove();
    }

    private static void fire() {
        for (Runnable listener : LISTENERS) {
            try {
                listener.run();
            } catch (Throwable ignored) {
                // a wakeup must never break the publish path
            }
        }
    }
}
