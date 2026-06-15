package org.kendar.cqrses.scheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.annotations.Schedulable;
import org.kendar.cqrses.annotations.Schedule;
import org.kendar.cqrses.db.AbstractJdbcTest;
import org.kendar.cqrses.db.UuidBytes;
import org.kendar.cqrses.di.GlobalRegistry;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

class JdbcSchedulerTest extends AbstractJdbcTest {

    private Recorder recorder;
    private JdbcScheduler scheduler;

    @BeforeEach
    void setUp() {
        recorder = new Recorder();
        GlobalRegistry.register(Recorder.class, recorder);
        scheduler = new JdbcScheduler(db);
    }

    @AfterEach
    void tearDown() {
        if (scheduler != null && scheduler.isRunning()) scheduler.stop();
    }

    private static boolean await(BooleanSupplier cond, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return true;
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return cond.getAsBoolean();
    }

    @Test
    void firesNamedTaskWithDeserialisedParams() {
        scheduler.start();
        scheduler.schedule(Instant.now(), "greet", new Greeting("hello"));
        assertTrue(await(() -> recorder.messages.contains("hello"), 2000),
                "the @Schedule method must run with the deserialised params");
    }

    @Test
    void runnableOverloadIsUnsupported() {
        scheduler.start();
        assertThrows(UnsupportedOperationException.class,
                () -> scheduler.schedule(Instant.now(), () -> {
                }));
    }

    @Test
    void schedulingUnknownTaskNameThrows() {
        scheduler.start();
        assertThrows(IllegalArgumentException.class,
                () -> scheduler.schedule(Instant.now(), "nope", new Greeting("x")));
    }

    @Test
    void cancelRemovesPendingTask() {
        scheduler.start();
        UUID id = scheduler.schedule(Instant.now().plusSeconds(3600), "greet", new Greeting("later"));
        assertTrue(scheduler.cancel(id), "pending task should cancel");
        assertFalse(scheduler.cancel(id), "second cancel finds nothing");
        // give the poller a moment; it must never fire a cancelled task
        assertFalse(await(() -> recorder.messages.contains("later"), 300));
    }

    @Test
    void disjointOwnershipFiresEachTaskExactlyOnce() {
        // The scheduler is a segment-owned workload: there is no cross-JVM OCC. Two pollers
        // over one DB must own DISJOINT segments — then each task's segment has exactly one
        // owner, so it fires exactly once. (Two pollers both owning all segments would
        // double-fire; that is an invalid deployment, not a property to assert.)
        JdbcScheduler even = new JdbcScheduler(db) {
            @Override
            protected boolean ownsSegment(int segment) {
                return segment % 2 == 0;
            }
        };
        JdbcScheduler odd = new JdbcScheduler(db) {
            @Override
            protected boolean ownsSegment(int segment) {
                return segment % 2 != 0;
            }
        };
        try {
            even.start();
            odd.start();
            int n = 12;
            for (int i = 0; i < n; i++) {
                even.schedule(Instant.now(), "count", new Greeting("tick"));
            }
            assertTrue(await(() -> recorder.count.get() >= n, 3000), "all tasks must fire");
            // settle, then prove none double-fired across the two pollers
            await(() -> false, 300);
            assertEquals(n, recorder.count.get(),
                    "each task's segment has exactly one owner, so it fires exactly once");
        } finally {
            even.stop();
            odd.stop();
        }
    }

    @Test
    void throwingTaskAtCapIsDroppedNotRetried() {
        scheduler.start();
        UUID id = scheduler.schedule(Instant.now(), "boom", new Greeting("x"));
        assertTrue(await(() -> recorder.boomAttempts.get() >= 1, 2000), "task must be attempted");
        // Default cap is one attempt: a deterministic throw is logged and the row dropped,
        // never hot-looped.
        assertTrue(await(() -> db.queryForObject(
                        "SELECT COUNT(*) FROM scheduled_task WHERE id = ?", Long.class,
                        UuidBytes.toBytes(id)) == 0L, 1000),
                "a throwing task at the cap must be dropped");
        await(() -> false, 300);
        assertEquals(1, recorder.boomAttempts.get(), "dropped task must not be retried");
    }

    @Test
    void throwingTaskRetriesWithBackoffThenSucceeds() {
        JdbcScheduler retrying = new JdbcScheduler(db, 3) {
            @Override
            protected long backoffMillis(int attempts) {
                return 50; // keep the test fast
            }
        };
        try {
            retrying.start();
            UUID id = retrying.schedule(Instant.now(), "flaky", new Greeting("done"));
            // flaky throws on attempts 1 and 2, succeeds on 3 (still under the cap of 3).
            assertTrue(await(() -> recorder.flakyMessages.contains("done"), 4000),
                    "task must eventually succeed after backing off and retrying");
            assertTrue(recorder.flakyInvocations.get() >= 3, "it must have retried before succeeding");
            assertTrue(await(() -> db.queryForObject(
                            "SELECT COUNT(*) FROM scheduled_task WHERE id = ?", Long.class,
                            UuidBytes.toBytes(id)) == 0L, 1000),
                    "a task that finally succeeds is deleted");
        } finally {
            retrying.stop();
        }
    }

    @Test
    void persistedTaskSurvivesSchedulerRestart() {
        // Persist a task with the first scheduler, then stop it before it is due.
        scheduler.start();
        UUID id = scheduler.schedule(Instant.now().plusSeconds(3600), "greet", new Greeting("survivor"));
        scheduler.stop();
        assertFalse(recorder.messages.contains("survivor"), "must not have fired yet");

        // Row is still there (durable across the scheduler's lifecycle).
        Long pending = db.queryForObject(
                "SELECT COUNT(*) FROM scheduled_task WHERE id = ?", Long.class, UuidBytes.toBytes(id));
        assertEquals(1L, pending);

        // Make it due and bring up a fresh scheduler over the same database.
        db.update("UPDATE scheduled_task SET execution_time = ? WHERE id = ?",
                Instant.now().minusSeconds(1).toEpochMilli(), UuidBytes.toBytes(id));

        JdbcScheduler restarted = new JdbcScheduler(db);
        try {
            restarted.start();
            assertTrue(await(() -> recorder.messages.contains("survivor"), 2000),
                    "a restarted scheduler must resume firing persisted rows");
            assertTrue(await(() -> db.queryForObject(
                    "SELECT COUNT(*) FROM scheduled_task WHERE id = ?", Long.class,
                    UuidBytes.toBytes(id)) == 0L, 1000), "fired task row is deleted on success");
        } finally {
            restarted.stop();
        }
    }

    @Schedulable
    public static class Recorder {
        final List<String> messages = new CopyOnWriteArrayList<>();
        final AtomicInteger count = new AtomicInteger();
        final AtomicInteger boomAttempts = new AtomicInteger();
        final AtomicInteger flakyInvocations = new AtomicInteger();
        final List<String> flakyMessages = new CopyOnWriteArrayList<>();

        @Schedule("greet")
        public void greet(Greeting g) {
            messages.add(g.message);
        }

        @Schedule("count")
        public void count(Greeting g) {
            count.incrementAndGet();
            messages.add(g.message);
        }

        @Schedule("boom")
        public void boom(Greeting g) {
            boomAttempts.incrementAndGet();
            throw new RuntimeException("boom");
        }

        @Schedule("flaky")
        public void flaky(Greeting g) {
            int n = flakyInvocations.incrementAndGet();
            if (n < 3) throw new RuntimeException("flaky #" + n);
            flakyMessages.add(g.message);
        }
    }

    public static class Greeting {
        public String message;

        public Greeting() {
        }

        public Greeting(String message) {
            this.message = message;
        }
    }
}
