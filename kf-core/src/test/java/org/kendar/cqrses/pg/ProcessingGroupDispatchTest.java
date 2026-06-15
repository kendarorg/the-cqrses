package org.kendar.cqrses.pg;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.annotations.Saga;
import org.kendar.cqrses.bus.Context;
import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.dlq.DlqEnqueueDecisionResult;
import org.kendar.cqrses.dlq.DlqItem;
import org.kendar.cqrses.dlq.DlqItemStatus;
import org.kendar.cqrses.repositories.SagaInstance;
import org.kendar.cqrses.repositories.SagaStore;
import org.kendar.cqrses.saga.SagaManager;
import org.kendar.cqrses.utils.TriConsumer;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.kendar.cqrses.pg.PgTestKit.*;

/**
 * Exercises {@link ProcessingGroup#invokeConsumers} — the dispatch +
 * error-routing core shared by the async worker loop and the synchronous send
 * path. Branch coverage: blocked-sequence short-circuit, happy-path dispatch,
 * and the IGNORE / EVICT / ENQUEUE decisions on handler failure.
 */
class ProcessingGroupDispatchTest {

    private static final String SEQ = "seq-1";
    private static final String GROUP = "orders";

    private RecordingDlqStore dlq;
    private Object messageObject;
    private TestBus bus;
    private FixedSerializer serializer;

    @Saga
    static class TestSaga {
    }

    static class TestProjection {
    }

    @BeforeEach
    void setUp() {
        GlobalRegistry.clear();
        SagaManager.clear();
        dlq = new RecordingDlqStore();
        messageObject = new Object();
        serializer = new FixedSerializer(messageObject);
        bus = new TestBus(serializer);
        bus.messageClassFn = name -> Object.class;
    }

    @AfterEach
    void tearDown() {
        GlobalRegistry.clear();
        SagaManager.clear();
    }

    private ProcessingGroup group(boolean commandSide, DlqEnqueueDecisionResult decision) {
        var policy = policy(GROUP, decision, SEQ);
        return new ProcessingGroup(GROUP, bus, serializer, commandSide, dlq, null, policy);
    }

    private static InternalMessage msg(UUID aggregateId) {
        return message("OrderPlaced", aggregateId);
    }

    // --- blocked sequence short-circuit ---------------------------------------

    @Test
    void blockedSequenceRoutesStraightToDlqWithoutInvokingHandler() {
        dlq.blocked = true;
        var invoked = new AtomicInteger();
        TriConsumer<Object, Object, Context> handler = (t, m, c) -> invoked.incrementAndGet();
        var pg = group(false, DlqEnqueueDecisionResult.enqueue());

        pg.invokeConsumers(msg(UUID.randomUUID()),
                List.of(registration(TestProjection.class, handler)));

        assertEquals(0, invoked.get(), "handler must not run for a blocked sequence");
        assertEquals(1, dlq.added.size());
        DlqItem item = dlq.added.get(0);
        assertEquals(DlqItemStatus.PENDING, item.getStatus());
        assertEquals(GROUP, item.getProcessingGroup());
        assertEquals(SEQ, item.getSequenceId());
        assertNull(item.getErrorClass(), "blocked-path item carries no exception");
        assertTrue(item.getErrorMessage().startsWith("Blocked by processing group " + GROUP));
        assertNotNull(item.getId());
    }

    // --- happy path -----------------------------------------------------------

    @Test
    void eventSideInvokesEveryConsumerInOrder() {
        var order = new StringBuilder();
        var pg = group(false, DlqEnqueueDecisionResult.ignore());

        pg.invokeConsumers(msg(UUID.randomUUID()), List.of(
                registration(TestProjection.class, (t, m, c) -> order.append("a")),
                registration(TestProjection.class, (t, m, c) -> order.append("b"))));

        assertEquals("ab", order.toString());
        assertTrue(dlq.added.isEmpty());
        assertTrue(dlq.evicted.isEmpty());
    }

    @Test
    void handlerReceivesDeserializedMessageAndContext() {
        var seenTarget = new Object[1];
        var seenMessage = new Object[1];
        var seenCtx = new Context[1];
        bus.findTargetFn = (m, r) -> "the-target";
        var pg = group(false, DlqEnqueueDecisionResult.ignore());
        var message = msg(UUID.randomUUID());

        pg.invokeConsumers(message, List.of(registration(TestProjection.class, (t, m, c) -> {
            seenTarget[0] = t;
            seenMessage[0] = m;
            seenCtx[0] = c;
        })));

        assertEquals("the-target", seenTarget[0]);
        assertSame(messageObject, seenMessage[0]);
        assertSame(message.getContext(), seenCtx[0]);
    }

    // --- failure: IGNORE ------------------------------------------------------

    @Test
    void ignoreDecisionSwallowsFailureAndContinues() {
        var second = new AtomicInteger();
        var pg = group(false, DlqEnqueueDecisionResult.ignore());

        assertDoesNotThrow(() -> pg.invokeConsumers(msg(UUID.randomUUID()), List.of(
                registration(TestProjection.class, (t, m, c) -> {
                    throw new RuntimeException("handler boom");
                }),
                registration(TestProjection.class, (t, m, c) -> second.incrementAndGet()))));

        assertEquals(1, second.get(), "IGNORE continues to the next consumer");
        assertTrue(dlq.added.isEmpty());
        assertTrue(dlq.evicted.isEmpty());
    }

    // --- failure: EVICT -------------------------------------------------------

    @Test
    void evictDecisionEvictsFirstAndBreaks() {
        var second = new AtomicInteger();
        var pg = group(false, DlqEnqueueDecisionResult.evict());

        pg.invokeConsumers(msg(UUID.randomUUID()), List.of(
                registration(TestProjection.class, (t, m, c) -> {
                    throw new RuntimeException("handler boom");
                }),
                registration(TestProjection.class, (t, m, c) -> second.incrementAndGet())));

        assertEquals(List.of(SEQ), dlq.evicted);
        assertEquals(0, second.get(), "EVICT breaks out of the consumer loop");
        assertTrue(dlq.added.isEmpty());
    }

    // --- failure: ENQUEUE -----------------------------------------------------

    @Test
    void enqueueDecisionAddsFailedItemWithUnwrappedException() {
        var pg = group(false, DlqEnqueueDecisionResult.enqueue());
        var message = msg(UUID.randomUUID());

        pg.invokeConsumers(message,
                List.of(registration(TestProjection.class, (t, m, c) -> {
                    throw new IllegalStateException("handler boom");
                })));

        assertEquals(1, dlq.added.size());
        DlqItem item = dlq.added.get(0);
        assertEquals("handler boom", item.getErrorMessage(), "WRAP wrapper must be unwrapped");
        assertEquals(IllegalStateException.class.getName(), item.getErrorClass());
        assertNotNull(item.getStackTrace());
        assertFalse(item.getStackTrace().isEmpty());
        assertEquals(DlqItemStatus.PENDING, item.getStatus());
        assertEquals(SEQ, item.getSequenceId());
        // The failure Context is captured so a DlqManager can rebuild the message on retry.
        assertSame(message.getContext(), item.getProcessingContext());
    }

    @Test
    void blockedPathAlsoCapturesProcessingContext() {
        dlq.blocked = true;
        var pg = group(false, DlqEnqueueDecisionResult.enqueue());
        var message = msg(UUID.randomUUID());

        pg.invokeConsumers(message, List.of(registration(TestProjection.class, (t, m, c) -> {
        })));

        assertEquals(1, dlq.added.size());
        assertSame(message.getContext(), dlq.added.get(0).getProcessingContext());
    }

    // --- null target routing --------------------------------------------------

    @Test
    void nullTargetForNonSagaThrowsAndRoutesToDlq() {
        bus.findTargetFn = (m, r) -> null;
        var pg = group(false, DlqEnqueueDecisionResult.enqueue());

        pg.invokeConsumers(msg(UUID.randomUUID()),
                List.of(registration(TestProjection.class, (t, m, c) -> fail("must not dispatch"))));

        assertEquals(1, dlq.added.size());
        assertTrue(dlq.added.get(0).getErrorMessage().contains("Cannot find stored item"));
    }

    @Test
    void nullTargetForSagaIsSkippedSilently() {
        GlobalRegistry.register(TestSaga.class, null); // sets SAGA target type, no auto-subscribe
        bus.findTargetFn = (m, r) -> null;
        var pg = group(false, DlqEnqueueDecisionResult.enqueue());

        assertDoesNotThrow(() -> pg.invokeConsumers(msg(UUID.randomUUID()),
                List.of(registration(TestSaga.class, (t, m, c) -> fail("saga has no correlation match")))));

        assertTrue(dlq.added.isEmpty(), "an unmatched saga is not a failure");
        assertTrue(dlq.evicted.isEmpty());
    }

    // --- deserialize failure before sequence id is known ----------------------

    @Test
    void deserializeFailureWithoutSequenceIdIsLoggedNotEnqueued() {
        var throwingBus = new TestBus(new ThrowingSerializer());
        throwingBus.messageClassFn = name -> Object.class;
        var policy = policy(GROUP, DlqEnqueueDecisionResult.enqueue(), SEQ);
        var pg = new ProcessingGroup(GROUP, throwingBus, new ThrowingSerializer(), false, dlq, null, policy);

        assertDoesNotThrow(() -> pg.invokeConsumers(msg(UUID.randomUUID()),
                List.of(registration(TestProjection.class, (t, m, c) -> fail("never reached")))));

        assertTrue(dlq.added.isEmpty(), "no sequenceId yet, so nothing can be enqueued");
        assertTrue(dlq.evicted.isEmpty());
    }

    // --- saga store interaction on the event side -----------------------------

    static class RecordingSagaStore implements SagaStore {
        Object stored;
        Object deleted;

        @Override
        public void storeSaga(Object saga) {
            stored = saga;
        }

        @Override
        public void deleteSaga(Object saga) {
            deleted = saga;
        }

        @Override
        public Optional<SagaInstance> loadSaga(String sagaId) {
            return Optional.empty();
        }

        @Override
        public Optional<SagaInstance> loadSagaByCorrelationId(String correlationId, String type) {
            return Optional.empty();
        }
    }

    @Test
    void sagaTargetIsPersistedAfterHandlerWhenNotEnded() {
        GlobalRegistry.register(TestSaga.class, null);
        var sagaStore = new RecordingSagaStore();
        GlobalRegistry.register(SagaStore.class, sagaStore);
        var saga = new TestSaga();
        bus.findTargetFn = (m, r) -> saga;
        var pg = group(false, DlqEnqueueDecisionResult.ignore());

        pg.invokeConsumers(msg(UUID.randomUUID()),
                List.of(registration(TestSaga.class, (t, m, c) -> { /* no endSaga */ })));

        assertSame(saga, sagaStore.stored, "a live saga is stored back after the handler");
        assertNull(sagaStore.deleted);
        assertFalse(SagaManager.isSagaEnded(), "dispatcher clears the end flag afterwards");
    }

    @Test
    void endedSagaWithDeleteAfterCompletionIsDeleted() {
        GlobalRegistry.register(TestSaga.class, null);
        var sagaStore = new RecordingSagaStore();
        GlobalRegistry.register(SagaStore.class, sagaStore);
        var saga = new TestSaga();
        bus.findTargetFn = (m, r) -> saga;
        var pg = group(false, DlqEnqueueDecisionResult.ignore());

        pg.invokeConsumers(msg(UUID.randomUUID()),
                List.of(registration(TestSaga.class, (t, m, c) -> SagaManager.endSaga())));

        assertSame(saga, sagaStore.deleted, "@Saga(deleteAfterCompletion=true) + endSaga deletes");
        assertNull(sagaStore.stored);
        assertFalse(SagaManager.isSagaEnded(), "end flag is cleared in finally");
    }

    // --- command side: single-handler semantics -------------------------------

    @Test
    void commandSideStopsAfterFirstConsumer() {
        var first = new AtomicInteger();
        var second = new AtomicInteger();
        var pg = group(true, DlqEnqueueDecisionResult.ignore());

        pg.invokeConsumers(msg(UUID.randomUUID()), List.of(
                registration(TestProjection.class, (t, m, c) -> first.incrementAndGet()),
                registration(TestProjection.class, (t, m, c) -> second.incrementAndGet())));

        assertEquals(1, first.get());
        assertEquals(0, second.get(), "command dispatch returns after the first handler");
        assertTrue(dlq.added.isEmpty());
    }
}
