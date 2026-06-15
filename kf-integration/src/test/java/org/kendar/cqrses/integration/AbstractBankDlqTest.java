package org.kendar.cqrses.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.bus.Bus;
import org.kendar.cqrses.bus.CommandBus;
import org.kendar.cqrses.bus.EventBus;
import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.dlq.*;
import org.kendar.cqrses.exceptions.DlqRetryFailedException;
import org.kendar.cqrses.integration.bank.*;
import org.kendar.cqrses.pg.PerAggregateSequencePolicy;
import org.kendar.cqrses.repositories.EventStore;
import org.kendar.cqrses.repositories.SagaStore;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.serialization.UpcastersManager;
import org.kendar.cqrses.utils.UUIDGenerator;

import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Dead-letter-queue handling for the bank example, expressed against the
 * processing-group DLQ model in {@code kf-core} and independently of which store
 * and bus implementation backs it.
 *
 * <p>The {@link FraudMonitor} projection runs in its own processing group
 * ("fraud"). That group is configured with a {@link DlqEnqueuePolicy} that
 * routes any failed event to the {@link DlqStore}, and a
 * {@link PerAggregateSequencePolicy} so the DLQ sequence id is the failing
 * account — giving per-account head-of-line blocking. A deposit to an account
 * under a fraud hold throws, so the {@code Deposited} event is dead-lettered
 * under that account's sequence while the "balances" group keeps flowing.
 *
 * <p>There is no operator retry/dismiss API in this model: a dead letter is
 * resolved by {@link DlqStore#evictFirst(String)}, which clears the block so
 * the aggregate's events flow again.
 *
 * <p>The concrete store/bus implementation is supplied by {@link #createBackend()};
 * every implementation must make this suite pass. See
 * {@link org.kendar.cqrses.integration.memory.InMemoryBankDlqTest} for the
 * in-memory binding.
 *
 * @see AbstractBankLedgerTest for the happy-path wiring this builds on.
 */
public abstract class AbstractBankDlqTest {

    /** Supplies the implementation under test. One fresh backend per test. */
    protected abstract IntegrationBackend createBackend();

    private IntegrationBackend backend;
    private CommandBus commandBus;
    private EventBus eventBus;
    private EventStore eventStore;
    private SagaStore sagaStore;
    private DlqStore dlqStore;
    private BalanceProjection projection;
    private FraudMonitor fraudMonitor;
    private MessageSerializer serializer;
    private DlqManager dlqManager;
    private UpcastersManager upcasterManager;

    /**
     * Dead-letter any event whose handler threw; leave successful events alone.
     * This is the "fraud" group's failure policy — without it the group would
     * fall back to the default IGNORE policy and silently drop the failure.
     */
    static class DeadLetterOnFailure extends DlqEnqueuePolicy {
        @Override
        public DlqEnqueueDecisionResult shouldEnqueue(InternalMessage message, Throwable error) {
            return error != null ? DlqEnqueueDecisionResult.enqueue()
                    : DlqEnqueueDecisionResult.doNotEnqueue();
        }
    }

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
        // Registering the DlqStore is what makes the DLQ policy actually persist
        // dead letters instead of degrading to a logged drop.
        GlobalRegistry.register(DlqStore.class, dlqStore);

        commandBus = backend.newCommandBus(serializer);
        eventBus = backend.newEventBus(serializer);
        // "balances" keeps the default (IGNORE) policy: its handler never fails.
        eventBus.setProcessingGroupPolicy(Bus.defaultProcessingGroupPolicyConfig("balances"));
        // "fraud" dead-letters failures, keyed per account so one held account
        // blocks only its own stream.
        eventBus.setProcessingGroupPolicy(new Bus.ProcessingGroupPolicyConfig(
                "fraud", new DeadLetterOnFailure(), new PerAggregateSequencePolicy()));
        GlobalRegistry.register(CommandBus.class, commandBus);
        GlobalRegistry.register(EventBus.class, eventBus);

        projection = new BalanceProjection();
        fraudMonitor = new FraudMonitor();
        // Live instances: EventBus.findTarget for a PROJECTION resolves via
        // GlobalRegistry.get(class), so the same objects must be reachable.
        GlobalRegistry.register(BalanceProjection.class, projection);
        GlobalRegistry.register(FraudMonitor.class, fraudMonitor);

        GlobalRegistry.register(AccountAggregate.class);

        org.kendar.cqrses.pg.ProcessingGroupsManager eventHandler = backend.handlerOf(eventBus);
        if (backend.eventSidePull()) eventHandler.setPullMode(true);
        commandBus.start();
        eventBus.start();
        if (backend.eventSidePull()) backend.startEventSide(eventHandler);

        // Operator tool over the event bus: re-invokes the live FraudMonitor in an
        // isolated processing group, or hands work back to the live worker.
        dlqManager = new LocalDlqManager(eventBus, dlqStore, backend.handlerOf(eventBus));
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

    /** The fraud group keys its DLQ by account, so a letter lives under the account id. */
    private DlqItem awaitOneFraudLetter(UUID account) {
        awaitUntil(() -> !dlqStore.listItems(account.toString()).isEmpty());
        var items = dlqStore.listItems(account.toString());
        assertEquals(1, items.size(), "exactly one fraud dead letter expected");
        return items.get(0);
    }

    private void openAndHold(UUID account) {
        commandBus.sendSync(new OpenAccount(account));
        fraudMonitor.hold(account);
    }

    /**
     * A deposit to a held account is dead-lettered by the "fraud" group — with the
     * failing event type, aggregate id and originating exception recorded — while
     * the "balances" group still advances the ledger. The two groups are isolated.
     */
    @Test
    void depositToHeldAccountIsDeadLetteredWhileBalancesAdvances() {
        UUID account = UUIDGenerator.newUuid();
        openAndHold(account);

        commandBus.send(new Deposit(account, 100, null));

        DlqItem letter = awaitOneFraudLetter(account);
        assertEquals(DlqItemStatus.PENDING, letter.getStatus());
        assertEquals("Deposited", letter.getEventType());
        assertEquals(account, letter.getAggregateId());
        assertEquals("fraud", letter.getProcessingGroup());
        assertEquals(IllegalStateException.class.getName(), letter.getErrorClass());
        assertTrue(letter.getErrorMessage().contains("fraud hold"),
                "the original handler exception is recorded on the letter");

        // The failing group is isolated: "balances" advanced past the same event,
        // so the ledger already shows the deposit even though compliance did not
        // clear it.
        assertEquals(100, projection.balanceOf(account), "balances group advanced despite the fraud failure");
        assertEquals(0, fraudMonitor.clearedTotal(account), "compliance has not cleared the held deposit");
    }

    /**
     * Once an account has a dead letter, head-of-line blocking routes every later
     * event for that same account straight to the DLQ — even after the hold is
     * lifted — until the block is cleared. The blocked letter carries no exception.
     */
    @Test
    void furtherEventsForABlockedAccountAreAlsoDeadLettered() {
        UUID account = UUIDGenerator.newUuid();
        openAndHold(account);
        commandBus.send(new Deposit(account, 50, null));
        awaitOneFraudLetter(account);

        // Root cause "fixed", but the account's stream is still blocked.
        fraudMonitor.release(account);
        commandBus.send(new Deposit(account, 60, null));

        awaitUntil(() -> dlqStore.listItems(account.toString()).size() == 2);
        var letters = dlqStore.listItems(account.toString());
        DlqItem blocked = letters.get(1);
        assertEquals("Deposited", blocked.getEventType());
        assertNull(blocked.getErrorClass(), "the blocked event itself did not fail");
        assertTrue(blocked.getErrorMessage().startsWith("Blocked by processing group fraud"));
        assertEquals(0, fraudMonitor.clearedTotal(account),
                "nothing clears while the account is blocked, even with the hold lifted");
    }

    /**
     * Evicting the head of a blocked account's DLQ sequence clears the block, so a
     * subsequent deposit (with the hold lifted) flows through and is cleared by
     * compliance.
     */
    @Test
    void evictingTheDeadLetterUnblocksTheAccount() {
        UUID account = UUIDGenerator.newUuid();
        openAndHold(account);
        commandBus.send(new Deposit(account, 40, null));
        awaitOneFraudLetter(account);

        // Operator lifts the hold and discards the dead letter.
        fraudMonitor.release(account);
        dlqStore.evictFirst(account.toString());
        assertFalse(dlqStore.hasBlockedItems(account.toString()), "block cleared after eviction");

        commandBus.send(new Deposit(account, 30, null));

        awaitUntil(() -> fraudMonitor.clearedTotal(account) == 30);
        assertEquals(30, fraudMonitor.clearedTotal(account),
                "a fresh deposit flows once the account is unblocked");
    }

    /**
     * A hold on one account dead-letters only that account: a deposit to a
     * different, un-held account is cleared normally and never touches the DLQ.
     */
    @Test
    void aHoldOnOneAccountDoesNotBlockAnother() {
        UUID held = UUIDGenerator.newUuid();
        UUID healthy = UUIDGenerator.newUuid();
        openAndHold(held);
        commandBus.sendSync(new OpenAccount(healthy));

        commandBus.send(new Deposit(held, 100, null));
        commandBus.send(new Deposit(healthy, 200, null));

        awaitOneFraudLetter(held);
        awaitUntil(() -> fraudMonitor.clearedTotal(healthy) == 200);
        assertTrue(dlqStore.listItems(healthy.toString()).isEmpty(),
                "the healthy account never dead-letters");
    }

    /**
     * Operator fixes the root cause (lifts the hold) and retries the dead letter:
     * the deposit is re-processed by compliance, the letter is resolved, the block
     * is cleared, and the account's stream flows again.
     */
    @Test
    void retryResolvesTheLetterOnceTheHoldIsLifted() {
        UUID account = UUIDGenerator.newUuid();
        openAndHold(account);
        commandBus.send(new Deposit(account, 100, null));
        DlqItem letter = awaitOneFraudLetter(account);

        fraudMonitor.release(account);
        dlqManager.retry(letter.getId()); // synchronous — runs on this thread

        assertEquals(DlqItemStatus.RESOLVED, letter.getStatus());
        assertEquals(100, fraudMonitor.clearedTotal(account), "the dead-lettered deposit is now cleared");
        assertFalse(dlqStore.hasBlockedItems(account.toString()), "block cleared");

        // The account flows again.
        commandBus.send(new Deposit(account, 30, null));
        awaitUntil(() -> fraudMonitor.clearedTotal(account) == 130);
    }

    /**
     * Retrying while the hold is still in place fails again: the manager throws, the
     * attempt is recorded on the letter (retryCount bumped, last-retry error stored),
     * and the letter stays PENDING and blocking.
     */
    @Test
    void retryWhileStillHeldThrowsAndStaysPending() {
        UUID account = UUIDGenerator.newUuid();
        openAndHold(account);
        commandBus.send(new Deposit(account, 50, null));
        DlqItem letter = awaitOneFraudLetter(account);

        assertThrows(DlqRetryFailedException.class, () -> dlqManager.retry(letter.getId()));

        assertEquals(1, letter.getRetryCount());
        assertEquals(DlqItemStatus.PENDING, letter.getStatus());
        assertEquals(IllegalStateException.class.getName(), letter.getLastRetryErrorClass());
        assertTrue(letter.getLastRetryErrorMessage().contains("fraud hold"));
        assertTrue(dlqStore.hasBlockedItems(account.toString()), "still blocking after a failed retry");
        assertEquals(0, fraudMonitor.clearedTotal(account));
    }

    /**
     * Dismissing a dead letter closes it and clears the block without re-processing;
     * the held deposit is never cleared by compliance.
     */
    @Test
    void dismissClosesTheLetterAndClearsTheBlockWithoutReprocessing() {
        UUID account = UUIDGenerator.newUuid();
        openAndHold(account);
        commandBus.send(new Deposit(account, 75, null));
        DlqItem letter = awaitOneFraudLetter(account);

        dlqManager.dismiss(letter.getId());

        assertEquals(DlqItemStatus.DISMISSED, letter.getStatus());
        assertFalse(dlqStore.hasBlockedItems(account.toString()));
        assertEquals(0, fraudMonitor.clearedTotal(account), "dismissed deposit is never cleared");

        // The account flows again for fresh deposits.
        fraudMonitor.release(account);
        commandBus.send(new Deposit(account, 10, null));
        awaitUntil(() -> fraudMonitor.clearedTotal(account) == 10);
    }

    /**
     * Redispatch clears the block and hands the message back to the LIVE fraud
     * worker, which (with the hold lifted) re-processes it asynchronously.
     */
    @Test
    void redispatchReprocessesViaTheLiveWorker() {
        UUID account = UUIDGenerator.newUuid();
        openAndHold(account);
        commandBus.send(new Deposit(account, 80, null));
        DlqItem letter = awaitOneFraudLetter(account);

        fraudMonitor.release(account);
        dlqManager.redispatch(letter.getId());

        awaitUntil(() -> fraudMonitor.clearedTotal(account) == 80);
        assertFalse(dlqStore.hasBlockedItems(account.toString()), "block cleared by redispatch");
    }
}
