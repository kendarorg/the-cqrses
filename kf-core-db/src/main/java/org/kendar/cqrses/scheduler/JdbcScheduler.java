package org.kendar.cqrses.scheduler;

import org.kendar.cqrses.annotations.Schedulable;
import org.kendar.cqrses.annotations.Schedule;
import org.kendar.cqrses.db.Db;
import org.kendar.cqrses.db.UuidBytes;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.pg.SegmentCalculator;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.utils.UUIDGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Durable, restart-safe {@link Scheduler}. Tasks are persisted as
 * {@code (task_name, segment, params_json)} rows in {@code scheduled_task}.
 * <p>
 * <b>The scheduler is just another segment-partitioned workload.</b> Each row is
 * sharded by {@code segment = SegmentCalculator.calculateSegment(taskId)} and only
 * the segment's <em>owner</em> polls it ({@link #ownsSegment(int)} — the seam the
 * cluster plugs ownership into; single-node owns every segment). Because exactly
 * one node owns a segment, there is no cross-JVM "exactly-once pick": the old
 * {@code PICKED}/{@code picked_by}/{@code version} OCC is gone. Firing is
 * <b>at-least-once</b>, aligned with the rest of the framework:
 * <ul>
 *   <li><b>Crash → re-fire (free).</b> A row is deleted only on success, so a crash
 *       mid-fire leaves it {@code execution_time <= now} for the segment's next
 *       owner to re-fire.</li>
 *   <li><b>Throw → backoff + cap, then drop.</b> A deterministic failure would
 *       otherwise hot-loop (the row stays due), so a throw bumps {@code attempts}
 *       and pushes {@code execution_time} out by {@link #backoffMillis(int)}; at
 *       {@code maxAttempts} (default 1) the error is logged and the row dropped —
 *       scheduled tasks are fire-and-forget, there is no DLQ for them.</li>
 * </ul>
 * A single daemon poll thread fires due, owned rows sequentially, so a row can
 * never be re-selected concurrently with its own in-flight fire on one node.
 * <p>
 * Dispatch is annotation-driven: at {@link #start()} it scans
 * {@code GlobalRegistry} instances for {@link Schedulable} beans and builds a
 * {@code taskName -> (instance, @Schedule method, paramType)} map. On fire the
 * row's {@code params_json} is deserialised to the method's single declared
 * parameter type and the method invoked reflectively.
 * <p>
 * The closure overload {@link #schedule(Instant, Runnable)} throws: a
 * {@code Runnable} is not persistable, which is the whole point.
 */
public class JdbcScheduler implements Scheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcScheduler.class.getName());
    private static final long POLL_MS = 100L;
    private static final int DEFAULT_MAX_ATTEMPTS = 1;
    private static final long BACKOFF_BASE_MS = 1_000L;
    private static final long BACKOFF_MAX_MS = 60_000L;
    private static final AtomicLong THREAD_INDEX = new AtomicLong();

    private final Db db;
    private final int maxAttempts;
    private final ConcurrentHashMap<String, TaskDef> tasks = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile Thread pollThread;

    public JdbcScheduler(Db db) {
        this(db, DEFAULT_MAX_ATTEMPTS);
    }

    /**
     * @param maxAttempts how many times a throwing task is attempted before it is
     *                    logged and dropped (default {@value #DEFAULT_MAX_ATTEMPTS}).
     *                    A crash (no throw) does not consume an attempt.
     */
    public JdbcScheduler(Db db, int maxAttempts) {
        this.db = db;
        this.maxAttempts = Math.max(1, maxAttempts);
    }

    private static MessageSerializer<?, ?> serializer() {
        return GlobalRegistry.get(MessageSerializer.class);
    }

    /**
     * Whether this node owns {@code segment} and should therefore poll its tasks.
     * Single-node owns every segment; the cluster overrides this with its live
     * owned-segment set so each scheduled task fires on exactly one node.
     */
    protected boolean ownsSegment(int segment) {
        return true;
    }

    /** Backoff applied to a throwing task before its next attempt; exponential, capped. */
    protected long backoffMillis(int attempts) {
        long shift = Math.min(attempts - 1, 16); // guard against overflow
        return Math.min(BACKOFF_MAX_MS, BACKOFF_BASE_MS << shift);
    }

    @Override
    public UUID schedule(Instant when, Runnable task) {
        throw new UnsupportedOperationException(
                "JdbcScheduler is durable; a Runnable cannot be persisted. " +
                        "Use schedule(Instant, String taskName, Object params).");
    }

    @Override
    public UUID schedule(Instant when, String taskName, Object params) {
        if (when == null || taskName == null) {
            throw new IllegalArgumentException("schedule: 'when' and 'taskName' must be non-null");
        }
        if (!tasks.containsKey(taskName)) {
            throw new IllegalArgumentException(
                    "Unknown scheduled task '" + taskName + "'. No @Schedulable bean registered in " +
                            "GlobalRegistry exposes an @Schedule(\"" + taskName + "\") method.");
        }
        UUID id = UUIDGenerator.newUuid();
        byte[] paramsJson = params == null ? null : serializer().serialize(params);
        db.insertInto("scheduled_task")
                .set("id", UuidBytes.toBytes(id))
                .set("task_name", taskName)
                .set("segment", SegmentCalculator.calculateSegment(id))
                .set("execution_time", when.toEpochMilli())
                .set("params_json", paramsJson)
                .set("attempts", 0)
                .execute();
        return id;
    }

    @Override
    public boolean cancel(UUID scheduleId) {
        int rows = db.update(
                "DELETE FROM scheduled_task WHERE id = ?", UuidBytes.toBytes(scheduleId));
        return rows > 0;
    }

    @Override
    public synchronized void start() {
        if (!running.compareAndSet(false, true)) return;
        scanSchedulables();
        Thread t = new Thread(this::pollLoop, "kf-jdbc-scheduler-" + THREAD_INDEX.incrementAndGet());
        t.setDaemon(true);
        pollThread = t;
        t.start();
        LOGGER.trace("JdbcScheduler started with " + tasks.size() + " task definition(s)");
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) return;
        Thread t = pollThread;
        pollThread = null;
        if (t != null) {
            t.interrupt();
            try {
                t.join(POLL_MS * 2);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        // Task definitions are rebuilt on next start(); persisted rows survive.
        tasks.clear();
        LOGGER.trace("JdbcScheduler stopped");
    }

    private void scanSchedulables() {
        tasks.clear();
        for (Object bean : GlobalRegistry.allInstances()) {
            Class<?> clazz = bean.getClass();
            if (!clazz.isAnnotationPresent(Schedulable.class)) continue;
            for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass()) {
                for (Method m : c.getDeclaredMethods()) {
                    Schedule sched = m.getAnnotation(Schedule.class);
                    if (sched == null) continue;
                    if (m.getParameterCount() != 1) {
                        throw new IllegalStateException(
                                "@Schedule method " + c.getName() + "." + m.getName() +
                                        " must declare exactly one parameter");
                    }
                    String name = sched.value();
                    m.setAccessible(true);
                    TaskDef prev = tasks.putIfAbsent(name,
                            new TaskDef(bean, m, m.getParameterTypes()[0]));
                    if (prev != null) {
                        throw new IllegalStateException(
                                "Duplicate @Schedule task name '" + name + "' on " +
                                        prev.instance.getClass().getName() + " and " + c.getName());
                    }
                }
            }
        }
    }

    private void pollLoop() {
        while (running.get()) {
            try {
                runDueTasks();
            } catch (Throwable t) {
                LOGGER.warn("JdbcScheduler poll threw", t);
            }
            try {
                Thread.sleep(POLL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void runDueTasks() {
        long now = Instant.now().toEpochMilli();
        List<DueRow> due = db.query(
                "SELECT id, segment, task_name, params_json, attempts FROM scheduled_task " +
                        "WHERE execution_time <= ? ORDER BY execution_time",
                (rs, rowNum) -> new DueRow(
                        rs.getBytes("id"), rs.getInt("segment"),
                        rs.getString("task_name"), rs.getBytes("params_json"), rs.getInt("attempts")),
                now);

        for (DueRow row : due) {
            if (!running.get()) return;
            // Only the segment's owner fires its tasks (single-node owns all).
            if (!ownsSegment(row.segment)) continue;
            try {
                // Deleted only on success: a crash before this leaves the row due to re-fire.
                // A row whose @Schedule definition is unknown here is left in place (not fired,
                // not dropped) for a node/redeploy that knows it.
                if (fire(row)) {
                    db.update("DELETE FROM scheduled_task WHERE id = ?", row.id);
                }
            } catch (Throwable t) {
                onThrow(row, now, t);
            }
        }
    }

    private void onThrow(DueRow row, long now, Throwable t) {
        int attempts = row.attempts + 1;
        UUID id = UuidBytes.fromBytes(row.id);
        if (attempts >= maxAttempts) {
            LOGGER.warn("Scheduled task '" + row.taskName + "' (" + id + ") threw on attempt " +
                    attempts + "/" + maxAttempts + "; dropping (no DLQ for scheduled tasks)", t);
            db.update("DELETE FROM scheduled_task WHERE id = ?", row.id);
        } else {
            long next = now + backoffMillis(attempts);
            db.update("UPDATE scheduled_task SET attempts = ?, execution_time = ? WHERE id = ?",
                    attempts, next, row.id);
            LOGGER.warn("Scheduled task '" + row.taskName + "' (" + id + ") threw on attempt " +
                    attempts + "/" + maxAttempts + "; backing off to " + Instant.ofEpochMilli(next), t);
        }
    }

    /** @return true if the task was actually invoked; false if its definition is unknown here. */
    private boolean fire(DueRow row) throws Exception {
        TaskDef def = tasks.get(row.taskName);
        if (def == null) {
            LOGGER.warn("No @Schedule definition for fired task '" + row.taskName +
                    "'; leaving row for a node that knows it");
            return false;
        }
        Object arg = row.paramsJson == null ? null
                : serializer().deserialize(row.paramsJson, def.paramType);
        try {
            def.method.invoke(def.instance, arg);
        } catch (InvocationTargetException ite) {
            // Surface the task's own failure, not the reflection wrapper.
            Throwable cause = ite.getCause() == null ? ite : ite.getCause();
            if (cause instanceof Exception e) throw e;
            if (cause instanceof Error err) throw err;
            throw ite;
        }
        return true;
    }

    private record TaskDef(Object instance, Method method, Class<?> paramType) {
    }

    private record DueRow(byte[] id, int segment, String taskName, byte[] paramsJson, int attempts) {
    }
}
