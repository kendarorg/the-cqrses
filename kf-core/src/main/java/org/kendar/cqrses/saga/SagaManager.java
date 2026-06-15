package org.kendar.cqrses.saga;

/**
 * Thread-local handshake between a saga's {@code @SagaHandler} and the
 * surrounding event-dispatch loop. Inside a handler, the saga calls
 * {@link #endSaga()} to mark itself as completed; after the handler returns
 * normally, the dispatcher reads {@link #isSagaEnded()} and — if the
 * {@code @Saga(deleteAfterCompletion = true)} flag is set — removes the saga
 * from the {@code SagaStore} instead of restoring it.
 *
 * <p>The flag lives in a {@link ThreadLocal} for the same reason
 * {@code EventApplyer}'s buffer does: the dispatcher invokes the handler on
 * its own worker thread and the saga has no other way to signal an outcome
 * without changing the {@code @SagaHandler} method signature. The dispatcher
 * is responsible for clearing the flag after every event so it never leaks
 * across invocations.
 */
public class SagaManager {

    private static final ThreadLocal<Boolean> ENDED = new ThreadLocal<>();

    private SagaManager() {
    }

    /**
     * Called from inside a {@code @SagaHandler} to mark the saga as completed.
     */
    public static void endSaga() {
        ENDED.set(Boolean.TRUE);
    }

    /**
     * Dispatcher-only. True iff the current event handler called {@link #endSaga()}.
     */
    public static boolean isSagaEnded() {
        return Boolean.TRUE.equals(ENDED.get());
    }

    /**
     * Dispatcher-only. Reset the flag after every saga-event invocation.
     */
    public static void clear() {
        ENDED.remove();
    }
}
