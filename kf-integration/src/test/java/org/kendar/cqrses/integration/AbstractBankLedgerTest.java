package org.kendar.cqrses.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.bus.*;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.dlq.DlqStore;
import org.kendar.cqrses.integration.bank.*;
import org.kendar.cqrses.repositories.EventStore;
import org.kendar.cqrses.repositories.SagaStore;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.serialization.UpcastersManager;
import org.kendar.cqrses.utils.UUIDGenerator;

import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * End-to-end test of the CQRS/ES wiring, expressed independently of which store
 * and bus implementation backs it: {@link AccountAggregate} +
 * {@link TransferAggregate} on the command side, {@link BalanceProjection} +
 * {@link TransferSaga} on the event side, with an {@link EventStore} and
 * {@link SagaStore} backing them. Aggregate handlers emit events via
 * {@link EventApplyer#apply}; the bus drains the per-command buffer, persists
 * the events, and republishes them so projections and sagas react.
 *
 * <p>The concrete store/bus implementation is supplied by {@link #createBackend()};
 * every implementation must make this suite pass. See
 * {@link org.kendar.cqrses.integration.memory.InMemoryBankLedgerTest} for the
 * in-memory binding.
 */
public abstract class AbstractBankLedgerTest {

    /** Supplies the implementation under test. One fresh backend per test. */
    protected abstract IntegrationBackend createBackend();

    private IntegrationBackend backend;
    private CommandBus commandBus;
    private EventBus eventBus;
    private EventStore eventStore;
    private SagaStore sagaStore;
    private DlqStore dlqStore;
    private BalanceProjection projection;
    private MessageSerializer<?, ?> serializer;
    private UpcastersManager upcasterManager;

    private static void awaitUntil(BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + 3_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return;
            try {
                Thread.sleep(10);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                fail("interrupted");
            }
        }
        fail("Condition did not become true within 3s");
    }

    @BeforeEach
    void setUp() {
        GlobalRegistry.clear();

        backend = createBackend();
        backend.start();

        serializer = new org.kendar.cqrses.serialization.JacksonMessageSerializer();
        upcasterManager = new UpcastersManager(serializer, List.of());
        GlobalRegistry.register(UpcastersManager.class, upcasterManager);
        GlobalRegistry.register(MessageSerializer.class, serializer);

        eventStore = backend.eventStore();
        sagaStore = backend.sagaStore();
        dlqStore = backend.dlqStore();
        GlobalRegistry.register(EventStore.class, eventStore);
        GlobalRegistry.register(SagaStore.class, sagaStore);
        commandBus = backend.newCommandBus(serializer);
        eventBus = backend.newEventBus(serializer);
        eventBus.setProcessingGroupPolicy(Bus.defaultProcessingGroupPolicyConfig("balances"));
        eventBus.setProcessingGroupPolicy(Bus.defaultProcessingGroupPolicyConfig("transfers-saga"));
        GlobalRegistry.register(CommandBus.class, commandBus);
        GlobalRegistry.register(EventBus.class, eventBus);

        projection = new BalanceProjection();
        // Live instance: EventBus.findTarget for PROJECTION resolves via
        // GlobalRegistry.get(class), so the same object must be reachable.
        GlobalRegistry.register(BalanceProjection.class, projection);

        // Aggregate + saga classes: class-only register so
        // registerAggregateEventHandlers / subscribeSaga run on their methods.
        GlobalRegistry.register(AccountAggregate.class);
        GlobalRegistry.register(TransferAggregate.class);
        GlobalRegistry.register(TransferSaga.class);

        org.kendar.cqrses.pg.ProcessingGroupsManager eventHandler = backend.handlerOf(eventBus);
        if (backend.eventSidePull()) eventHandler.setPullMode(true);
        commandBus.start();
        eventBus.start();
        if (backend.eventSidePull()) backend.startEventSide(eventHandler);
    }

    @AfterEach
    void tearDown() {
        try {
            backend.stopEventSide();
        } catch (Exception ignored) {
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
    void openAndDepositReachAggregateStoreAndProjection() {
        UUID accountA = UUIDGenerator.newUuid();
        commandBus.sendSync(new OpenAccount(accountA));
        commandBus.sendSync(new Deposit(accountA, 100, null));

        awaitUntil(() -> projection.balanceOf(accountA) == 100);

        // A bare loadEvents resolves the partition from the aggregate id's
        // segment alone (processing_group is recorded but not a read key, as in
        // JDBC), so no ambient group needs to be set.
        var stored = eventStore.loadEvents(accountA, -1);
        assertEquals(2, stored.size(), "AccountOpened + Deposited persisted");
        assertEquals("AccountOpened", stored.get(0).getContext().getType());
        assertEquals("Deposited", stored.get(1).getContext().getType());
        assertEquals(0, stored.get(0).getContext().getAggregateVersion());
        assertEquals(1, stored.get(1).getContext().getAggregateVersion());

        var rehydrated = eventStore.loadAggregate(accountA, AccountAggregate.class).orElseThrow();
        assertEquals(100, rehydrated.balance,
                "Aggregate rehydrated from stored events matches the projection");
    }

    @Test
    void successfulTransferRunsSagaWithdrawAndDeposit() {
        UUID a = UUIDGenerator.newUuid();
        UUID b = UUIDGenerator.newUuid();
        commandBus.sendSync(new OpenAccount(a));
        commandBus.sendSync(new OpenAccount(b));
        commandBus.sendSync(new Deposit(a, 100, null));
        awaitUntil(() -> projection.balanceOf(a) == 100);

        UUID transferId = UUIDGenerator.newUuid();
        commandBus.send(new RequestTransfer(transferId, a, b, 60));

        // The saga (group "transfers-saga") and the projection (group
        // "balances") consume Deposited on separate threads, so awaiting the
        // projection alone races the saga's on(Deposited) -> COMPLETED. Await
        // the saga's terminal state directly, as the failure-path test does.
        awaitUntil(() -> {
            var s = tryLoadSaga(transferId);
            return s != null && s.status == TransferSaga.Status.COMPLETED
                    && projection.balanceOf(a) == 40 && projection.balanceOf(b) == 60;
        });

        TransferSaga loaded = loadSaga(transferId);
        assertEquals(TransferSaga.Status.COMPLETED, loaded.status);
        assertEquals(transferId, loaded.transferId);

        // Cross-check: each aggregate's folded state matches the projection.
        assertEquals(projection.balanceOf(a),
                eventStore.loadAggregate(a, AccountAggregate.class).orElseThrow().balance);
        assertEquals(projection.balanceOf(b),
                eventStore.loadAggregate(b, AccountAggregate.class).orElseThrow().balance);
    }

    // ── helpers ────────────────────────────────────────────────────────────

    @Test
    void insufficientFundsLeavesBalancesUnchangedAndFailsTheSaga() {
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

        assertEquals(50, projection.balanceOf(a),
                "Withdraw was rejected — source balance unchanged");
        assertEquals(0, projection.balanceOf(b),
                "Target never received the deposit");
    }

    private TransferSaga loadSaga(UUID transferId) {
        var s = tryLoadSaga(transferId);
        if (s == null) fail("Saga for " + transferId + " not found");
        return s;
    }

    private TransferSaga tryLoadSaga(UUID transferId) {
        return sagaStore.loadSagaByCorrelationId(transferId.toString(),
                        TransferSaga.class.getSimpleName())
                .map(view -> serializer.deserialize(view.getContent(), TransferSaga.class))
                .orElse(null);
    }
}
