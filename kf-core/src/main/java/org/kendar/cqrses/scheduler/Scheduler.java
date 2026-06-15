package org.kendar.cqrses.scheduler;

import java.time.Instant;
import java.util.UUID;

/**
 * Single-fire scheduler used by the buses' {@code schedule()} methods.
 * Real implementations (Quartz, DB-backed pollers, k8s CronJobs) live in
 * their own modules; {@code InMemoryScheduler} is the reference implementation.
 * <p>
 * Only one-shot schedules are supported. Recurring schedules can be modelled
 * by having a task schedule its own next run before returning.
 */
public interface Scheduler {

    /**
     * Schedule {@code task} to run once at {@code when}.
     * If {@code when} is in the past, the task is eligible to run on the
     * next scheduler tick.
     *
     * @return an opaque id usable with {@link #cancel(UUID)}.
     */
    UUID schedule(Instant when, Runnable task);

    /**
     * Durable, restart-safe schedule keyed by a logical {@code taskName} (an
     * {@code @Schedule("...")} method on a {@code @Schedulable} bean) plus an
     * opaque {@code params} payload serialised via the registered
     * {@code MessageSerializer}. On fire the params are deserialised to the
     * method's declared parameter type and the method invoked reflectively.
     * <p>
     * Unlike {@link #schedule(Instant, Runnable)}, a {@code Runnable} closure is
     * not persistable; this overload exists so a scheduled task can survive a
     * process restart. The reference {@link Scheduler} is closure-based and
     * leaves this overload throwing {@link UnsupportedOperationException} by
     * default; durable implementations ({@code JdbcScheduler}) override it.
     *
     * @return an opaque id usable with {@link #cancel(UUID)}.
     */
    default UUID schedule(Instant when, String taskName, Object params) {
        throw new UnsupportedOperationException(
                "This Scheduler is not durable; use a persistent implementation for named tasks");
    }

    /**
     * Cancels a previously scheduled task. Returns true if the task was found
     * and not yet started; false otherwise (already ran, already cancelled,
     * unknown id).
     */
    boolean cancel(UUID scheduleId);

    /**
     * Starts the scheduler's background loop. Idempotent.
     */
    void start();

    /**
     * Stops the scheduler. Pending tasks are dropped. Idempotent.
     */
    void stop();

    /**
     * Whether the scheduler's background loop is currently running. Used by
     * the framework's liveness probe; default {@code true} so passive
     * implementations (one-shot triggers, externally-driven schedulers) do
     * not need to track a flag.
     */
    default boolean isRunning() {
        return true;
    }
}
