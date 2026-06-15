package org.kendar.cqrses.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.bus.*;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.dlq.DlqStore;
import org.kendar.cqrses.integration.bank.*;
import org.kendar.cqrses.pg.ProcessingGroupsManager;
import org.kendar.cqrses.pg.SegmentCalculator;
import org.kendar.cqrses.pg.SegmentProcessor;
import org.kendar.cqrses.repositories.CheckpointStore;
import org.kendar.cqrses.repositories.EventStore;
import org.kendar.cqrses.repositories.SagaStore;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.serialization.UpcastersManager;
import org.kendar.cqrses.utils.UUIDGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The cluster pull pump, end-to-end, against the bank fixtures. The event bus is
 * put in <b>pull mode</b> (no lane/saga-resolver threads); a single
 * {@link SegmentProcessor} claims <em>every</em> segment (the degenerate
 * one-node-owns-everything case) and is the only source of event-side dispatch —
 * polling the {@link EventStore} tail and driving projections and the saga.
 *
 * <p>Exercises Part 2 (projection pull), Part 4 (saga k-way merge + create-on-
 * {@code segment(trigger)} / update-on-{@code segment(sagaId)} split with
 * cross-segment correlation on {@code transferId}), and the pull-mode dispatch
 * switch. Both the in-memory and JDBC backends must pass.
 */
public abstract class AbstractClusterPullTest {

    protected abstract IntegrationBackend createBackend();

    private IntegrationBackend backend;
    private CommandBus commandBus;
    private EventBus eventBus;
    private EventStore eventStore;
    private SagaStore sagaStore;
    private DlqStore dlqStore;
    private CheckpointStore checkpointStore;
    private BalanceProjection projection;
    private MessageSerializer<?, ?> serializer;
    private SegmentProcessor segmentProcessor;
    private final List<Thread> claimers = new ArrayList<>();

    private static void awaitUntil(BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return;
            try {
                Thread.sleep(10);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                fail("interrupted");
            }
        }
        fail("Condition did not become true within 5s");
    }

    @BeforeEach
    void setUp() {
        GlobalRegistry.clear();

        backend = createBackend();
        backend.start();

        serializer = new org.kendar.cqrses.serialization.JacksonMessageSerializer();
        GlobalRegistry.register(UpcastersManager.class, new UpcastersManager(serializer, List.of()));
        GlobalRegistry.register(MessageSerializer.class, serializer);

        eventStore = backend.eventStore();
        sagaStore = backend.sagaStore();
        dlqStore = backend.dlqStore();
        checkpointStore = backend.newCheckpointStore();
        GlobalRegistry.register(EventStore.class, eventStore);
        GlobalRegistry.register(SagaStore.class, sagaStore);

        commandBus = backend.newCommandBus(serializer);
        eventBus = backend.newEventBus(serializer);
        eventBus.setProcessingGroupPolicy(Bus.defaultProcessingGroupPolicyConfig("balances"));
        eventBus.setProcessingGroupPolicy(Bus.defaultProcessingGroupPolicyConfig("transfers-saga"));
        GlobalRegistry.register(CommandBus.class, commandBus);
        GlobalRegistry.register(EventBus.class, eventBus);

        projection = new BalanceProjection();
        GlobalRegistry.register(BalanceProjection.class, projection);
        GlobalRegistry.register(AccountAggregate.class);
        GlobalRegistry.register(TransferAggregate.class);
        GlobalRegistry.register(TransferSaga.class);

        // Command side stays push; the EVENT side is pull — the SegmentProcessor
        // owns event dispatch. Pull mode must be set before the bus builds lanes.
        ProcessingGroupsManager eventHandler = backend.handlerOf(eventBus);
        eventHandler.setPullMode(true);

        commandBus.start();
        eventBus.start();

        segmentProcessor = new SegmentProcessor(eventHandler, eventStore, checkpointStore);
        for (int seg = 0; seg < SegmentCalculator.getSegments(); seg++) {
            final int s = seg;
            Thread t = new Thread(() -> segmentProcessor.claimSegment(s), "claim-" + s);
            t.setDaemon(true);
            claimers.add(t);
            t.start();
        }
    }

    @AfterEach
    void tearDown() {
        if (segmentProcessor != null) segmentProcessor.stopAll();
        for (Thread t : claimers) {
            try {
                t.join(1000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            commandBus.stop();
        } catch (Exception ignored) {
        }
        try {
            eventBus.stop();
        } catch (Exception ignored) {
        }
        try {
            backend.stop();
        } catch (Exception ignored) {
        }
        GlobalRegistry.clear();
    }

    @Test
    void projectionPull_openAndDepositReachProjection() {
        UUID accountA = UUIDGenerator.newUuid();
        commandBus.sendSync(new OpenAccount(accountA));
        commandBus.sendSync(new Deposit(accountA, 100, null));

        awaitUntil(() -> projection.balanceOf(accountA) == 100);
        assertEquals(100, projection.balanceOf(accountA));
    }

    @Test
    void sagaPull_crossSegmentTransferCompletes() {
        UUID a = UUIDGenerator.newUuid();
        UUID b = UUIDGenerator.newUuid();
        commandBus.sendSync(new OpenAccount(a));
        commandBus.sendSync(new OpenAccount(b));
        commandBus.sendSync(new Deposit(a, 100, null));
        awaitUntil(() -> projection.balanceOf(a) == 100);

        UUID transferId = UUIDGenerator.newUuid();
        commandBus.send(new RequestTransfer(transferId, a, b, 60));

        awaitUntil(() -> {
            var s = tryLoadSaga(transferId);
            return s != null && s.status == TransferSaga.Status.COMPLETED
                    && projection.balanceOf(a) == 40 && projection.balanceOf(b) == 60;
        });

        TransferSaga loaded = tryLoadSaga(transferId);
        assertEquals(TransferSaga.Status.COMPLETED, loaded.status);
        assertEquals(40, projection.balanceOf(a));
        assertEquals(60, projection.balanceOf(b));
    }

    @Test
    void sagaPull_insufficientFundsFailsTheSaga() {
        UUID a = UUIDGenerator.newUuid();
        UUID b = UUIDGenerator.newUuid();
        commandBus.sendSync(new OpenAccount(a));
        commandBus.sendSync(new OpenAccount(b));
        commandBus.sendSync(new Deposit(a, 50, null));
        awaitUntil(() -> projection.balanceOf(a) == 50);

        UUID transferId = UUIDGenerator.newUuid();
        commandBus.send(new RequestTransfer(transferId, a, b, 9999));

        awaitUntil(() -> {
            var s = tryLoadSaga(transferId);
            return s != null && s.status == TransferSaga.Status.FAILED;
        });

        assertEquals(50, projection.balanceOf(a));
        assertEquals(0, projection.balanceOf(b));
    }

    @Test
    void replay_skipsSagaGroupsByDefault_noSideEffectsReFire() {
        UUID a = UUIDGenerator.newUuid();
        UUID b = UUIDGenerator.newUuid();
        commandBus.sendSync(new OpenAccount(a));
        commandBus.sendSync(new OpenAccount(b));
        commandBus.sendSync(new Deposit(a, 100, null));
        awaitUntil(() -> projection.balanceOf(a) == 100);

        UUID transferId = UUIDGenerator.newUuid();
        commandBus.send(new RequestTransfer(transferId, a, b, 60));
        awaitUntil(() -> {
            var s = tryLoadSaga(transferId);
            return s != null && s.status == TransferSaga.Status.COMPLETED
                    && projection.balanceOf(a) == 40 && projection.balanceOf(b) == 60;
        });

        // Replaying the saga group with includeSagas=false must be a no-op: the saga
        // must NOT re-drive (which would re-emit Withdraw/Deposit and move money).
        segmentProcessor.replay(java.util.Set.of("transfers-saga"), 0L, false);
        try {
            Thread.sleep(300);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        assertEquals(40, projection.balanceOf(a), "saga side effects must not re-fire");
        assertEquals(60, projection.balanceOf(b), "saga side effects must not re-fire");
        assertEquals(TransferSaga.Status.COMPLETED, tryLoadSaga(transferId).status);
    }

    private TransferSaga tryLoadSaga(UUID transferId) {
        return sagaStore.loadSagaByCorrelationId(transferId.toString(),
                        TransferSaga.class.getSimpleName())
                .map(view -> serializer.deserialize(view.getContent(), TransferSaga.class))
                .orElse(null);
    }
}
