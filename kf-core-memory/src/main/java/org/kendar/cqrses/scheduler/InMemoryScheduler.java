package org.kendar.cqrses.scheduler;


import org.kendar.cqrses.annotations.Schedulable;
import org.kendar.cqrses.annotations.Schedule;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.utils.UUIDGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reference Scheduler — a single background thread that ticks every second and
 * runs any scheduled task whose due time has passed.
 * <p>
 * One thread, one tick per second.  Tasks themselves run on the tick thread,
 * so a slow task delays subsequent ticks; this is acceptable for the in-memory
 * reference but real implementations should isolate task execution.
 * <p>
 * The durable {@link #schedule(Instant, String, Object)} overload is implemented
 * in-process, mirroring {@code JdbcScheduler}'s fire path: it resolves the
 * {@code @Schedule(taskName)} method on a registered {@code @Schedulable} bean,
 * serialises {@code params} via the registered {@link MessageSerializer} and
 * deserialises to the method's declared parameter type on fire. Named tasks are
 * <strong>lost on restart</strong> like everything else in this module — full
 * scheduling-surface parity with JDBC minus durability.
 */
public class InMemoryScheduler implements Scheduler {


    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryScheduler.class.getName());

    private static final long TICK_MS = 100L;
    private static final AtomicLong THREAD_INDEX = new AtomicLong();

    private ConcurrentHashMap<UUID, ScheduledTask> tasks = new ConcurrentHashMap<>();
    // taskName -> (instance, @Schedule method, paramType), rebuilt on each start().
    private final ConcurrentHashMap<String, TaskDef> namedTasks = new ConcurrentHashMap<>();
    private AtomicBoolean running = new AtomicBoolean(false);
    private volatile Thread tickThread;

    @SuppressWarnings("rawtypes")
    private static MessageSerializer serializer() {
        return GlobalRegistry.get(MessageSerializer.class);
    }

    @Override
    public UUID schedule(Instant when, Runnable task) {
        if (when == null || task == null) {
            throw new IllegalArgumentException("schedule: 'when' and 'task' must be non-null");
        }
        if (!running.get()) {
            throw new IllegalStateException(
                    "InMemoryScheduler is not running — call start() before schedule(). " +
                            "Tasks queued on a stopped scheduler would never fire.");
        }
        UUID id = UUIDGenerator.newUuid();
        tasks.put(id, new ScheduledTask(when, task));
        LOGGER.trace("Scheduled task " + id + " for " + when);
        return id;
    }

    @Override
    @SuppressWarnings("unchecked")
    public UUID schedule(Instant when, String taskName, Object params) {
        if (when == null || taskName == null) {
            throw new IllegalArgumentException("schedule: 'when' and 'taskName' must be non-null");
        }
        if (!running.get()) {
            throw new IllegalStateException(
                    "InMemoryScheduler is not running — call start() before schedule(). " +
                            "Tasks queued on a stopped scheduler would never fire.");
        }
        TaskDef def = namedTasks.get(taskName);
        if (def == null) {
            throw new IllegalArgumentException(
                    "Unknown scheduled task '" + taskName + "'. No @Schedulable bean registered in " +
                            "GlobalRegistry exposes an @Schedule(\"" + taskName + "\") method.");
        }
        // Serialise the params now (as JDBC persists params_json) and deserialise
        // to the declared parameter type on fire — mirrors JdbcScheduler.fire so
        // both backends invoke the @Schedule method identically.
        byte[] paramsJson = params == null ? null : serializer().serialize(params);
        UUID id = UUIDGenerator.newUuid();
        Runnable task = () -> {
            try {
                Object arg = paramsJson == null ? null
                        : serializer().deserialize(paramsJson, def.paramType());
                def.method().invoke(def.instance(), arg);
            } catch (Exception e) {
                // Surface to the tick loop's catch, which logs it — matching the
                // closure path's failure handling.
                throw new RuntimeException("Scheduled task '" + taskName + "' threw", e);
            }
        };
        tasks.put(id, new ScheduledTask(when, task));
        LOGGER.trace("Scheduled named task '" + taskName + "' (" + id + ") for " + when);
        return id;
    }

    @Override
    public boolean cancel(UUID scheduleId) {
        ScheduledTask removed = tasks.remove(scheduleId);
        if (removed != null) {
            LOGGER.trace("Cancelled task " + scheduleId);
            return true;
        }
        return false;
    }

    @Override
    public synchronized void start() {
        if (!running.compareAndSet(false, true)) return;
        scanSchedulables();
        Thread t = new Thread(this::tickLoop, "kf-scheduler-" + THREAD_INDEX.incrementAndGet());
        t.setDaemon(true);
        tickThread = t;
        t.start();
        LOGGER.trace("InMemoryScheduler started");
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) return;
        Thread t = tickThread;
        tickThread = null;
        tasks.clear();
        namedTasks.clear();
        if (t != null) {
            t.interrupt();
            // Join so callers know the tick thread is actually gone before stop()
            // returns — otherwise a quick stop/start cycle can briefly run two
            // tick threads concurrently. The TICK_MS cap means the worst case is
            // ~1s of waiting for an in-flight task.
            try {
                t.join(TICK_MS * 2);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            if (t.isAlive()) {
                LOGGER.warn("InMemoryScheduler tick thread did not exit within "
                        + (TICK_MS * 2) + "ms — likely a long-running task is blocking shutdown");
            }
        }
        LOGGER.trace("InMemoryScheduler stopped");
    }

    private void tickLoop() {
        while (running.get()) {
            try {
                runDueTasks();
            } catch (Throwable t) {
                LOGGER.warn("Scheduler tick threw", t);
            }
            try {
                Thread.sleep(TICK_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void runDueTasks() {
        Instant now = Instant.now();
        Iterator<Map.Entry<UUID, ScheduledTask>> it = tasks.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<UUID, ScheduledTask> entry = it.next();
            ScheduledTask st = entry.getValue();
            if (!now.isBefore(st.when)) {
                it.remove();
                try {
                    st.task.run();
                } catch (Throwable t) {
                    LOGGER.warn("Scheduled task " + entry.getKey() + " threw", t);
                }
            }
        }
    }

    // Build taskName -> definition from registered @Schedulable beans, mirroring
    // JdbcScheduler.scanSchedulables so the two backends accept the same task names.
    private void scanSchedulables() {
        namedTasks.clear();
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
                    TaskDef prev = namedTasks.putIfAbsent(name,
                            new TaskDef(bean, m, m.getParameterTypes()[0]));
                    if (prev != null) {
                        throw new IllegalStateException(
                                "Duplicate @Schedule task name '" + name + "' on " +
                                        prev.instance().getClass().getName() + " and " + c.getName());
                    }
                }
            }
        }
    }

    private record ScheduledTask(Instant when, Runnable task) {
    }

    private record TaskDef(Object instance, Method method, Class<?> paramType) {
    }
}

