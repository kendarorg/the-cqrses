package org.kendar.cqrses.pg;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.bus.Bus;
import org.kendar.cqrses.bus.Context;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.saga.SagaManager;
import org.kendar.cqrses.utils.TriConsumer;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.kendar.cqrses.pg.PgTestKit.*;

class ProcessingGroupsManagerTest {

    static class TestProjection {
    }

    private RecordingDlqStore dlq;
    private TestBus bus;
    private ProcessingGroupsManager manager;

    @BeforeEach
    void setUp() {
        GlobalRegistry.clear();
        SagaManager.clear();
        dlq = new RecordingDlqStore();
        bus = new TestBus(new FixedSerializer(new Object()));
        bus.messageClassFn = name -> Object.class;
        bus.findTargetFn = (m, r) -> "target";
        manager = new ProcessingGroupsManager(bus, new FixedSerializer(new Object()), dlq);
    }

    @AfterEach
    void tearDown() {
        if (manager != null) manager.stop();
        GlobalRegistry.clear();
        SagaManager.clear();
    }

    private Map<String, Map<Class<?>, List<Bus.Registration>>> consumers(
            String group, TriConsumer<Object, Object, Context> handler) {
        return Map.of(group, Map.of(Object.class,
                List.of(registration(TestProjection.class, handler))));
    }

    @Test
    void startRegistersGroupsAndSpawnsLiveThreads() {
        var consumers = Map.of(
                "g1", Map.<Class<?>, List<Bus.Registration>>of(Object.class,
                        List.of(registration(TestProjection.class, (t, m, c) -> {
                        }))),
                "g2", Map.<Class<?>, List<Bus.Registration>>of(Object.class,
                        List.of(registration(TestProjection.class, (t, m, c) -> {
                        }))));

        manager.start(consumers);

        assertTrue(manager.handlesGroup("g1"));
        assertTrue(manager.handlesGroup("g2"));
        assertEquals(Set.of("g1", "g2"), manager.groups());
    }

    @Test
    void handlesGroupIsFalseForUnknownGroup() {
        manager.start(consumers("g1", (t, m, c) -> {
        }));
        assertFalse(manager.handlesGroup("nope"));
    }

    @Test
    void groupsReturnsAnImmutableSnapshot() {
        manager.start(consumers("g1", (t, m, c) -> {
        }));
        var groups = manager.groups();
        assertThrows(UnsupportedOperationException.class, () -> groups.add("x"));
    }

    @Test
    void sendDeliversMessageToWorkerHandler() throws InterruptedException {
        var latch = new CountDownLatch(1);
        var seenMessage = new AtomicReference<>();
        manager.start(consumers("g1", (t, m, c) -> {
            seenMessage.set(m);
            latch.countDown();
        }));

        manager.send(Set.of("g1"), message("OrderPlaced", UUID.randomUUID()));

        assertTrue(latch.await(2, TimeUnit.SECONDS), "worker never processed the enqueued message");
        assertNotNull(seenMessage.get());
    }

    @Test
    void sendAfterStopIsANoOp() throws InterruptedException {
        var invoked = new AtomicInteger();
        manager.start(consumers("g1", (t, m, c) -> invoked.incrementAndGet()));
        manager.stop();

        manager.send(Set.of("g1"), message("OrderPlaced", UUID.randomUUID()));

        Thread.sleep(150);
        assertEquals(0, invoked.get(), "a stopped group must not accept new work");
    }

    @Test
    void sendSyncInvokesHandlerOnTheCallerThread() {
        var invokingThread = new AtomicReference<Thread>();
        var consumers = consumers("g1", (t, m, c) -> invokingThread.set(Thread.currentThread()));
        manager.start(consumers);

        manager.sendSync(Set.of("g1"), consumers, message("OrderPlaced", UUID.randomUUID()));

        assertSame(Thread.currentThread(), invokingThread.get(),
                "sendSync must run handlers on the calling thread");
    }

    @Test
    void sendSyncSkipsStoppedGroups() {
        var invoked = new AtomicInteger();
        var consumers = consumers("g1", (t, m, c) -> invoked.incrementAndGet());
        manager.start(consumers);
        manager.stop();

        manager.sendSync(Set.of("g1"), consumers, message("OrderPlaced", UUID.randomUUID()));

        assertEquals(0, invoked.get(), "a stopped group is skipped by sync");
    }

    @Test
    void clearEmptiesPendingQueuesWithoutStopping() throws InterruptedException {
        // Block the only worker so the queue actually accumulates, then clear it.
        var gate = new CountDownLatch(1);
        var processed = new AtomicInteger();
        manager.start(consumers("g1", (t, m, c) -> {
            try {
                gate.await();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            processed.incrementAndGet();
        }));

        // Same aggregateId for every message so they share ONE segment lane;
        // otherwise they would fan out across separate lanes/threads.
        var aggId = UUID.randomUUID();
        // First message is picked up and blocks in the handler on the gate.
        manager.send(Set.of("g1"), message("OrderPlaced", aggId));
        Thread.sleep(100);
        // These pile up behind the blocked handler on the same lane.
        manager.send(Set.of("g1"), message("OrderPlaced", aggId));
        manager.send(Set.of("g1"), message("OrderPlaced", aggId));

        manager.clear();
        gate.countDown();
        Thread.sleep(150);

        // Only the in-flight message completes; the cleared ones never run.
        assertEquals(1, processed.get(), "clear() drops queued-but-unstarted messages");
    }
}
