package org.kendar.cqrses.test;

import com.fasterxml.jackson.databind.JsonNode;
import org.kendar.cqrses.annotations.Event;
import org.kendar.cqrses.annotations.Saga;
import org.kendar.cqrses.bus.Bus;
import org.kendar.cqrses.bus.CommandBus;
import org.kendar.cqrses.bus.Context;
import org.kendar.cqrses.bus.EventBus;
import org.kendar.cqrses.bus.FixtureBuses;
import org.kendar.cqrses.bus.InMemoryEventBus;
import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.cluster.spi.CommandForwarding;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.dlq.DlqEnqueueDecisionResult;
import org.kendar.cqrses.dlq.DlqEnqueuePolicy;
import org.kendar.cqrses.pg.NullSequencePolicy;
import org.kendar.cqrses.pg.SagaResolver;
import org.kendar.cqrses.pg.SegmentCalculator;
import org.kendar.cqrses.repositories.InMemoryDlqStore;
import org.kendar.cqrses.repositories.InMemorySagaStore;
import org.kendar.cqrses.repositories.SagaStore;
import org.kendar.cqrses.serialization.JacksonMessageSerializer;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.serialization.UpcastersManager;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Given–when–then unit-test fixture for one saga type:
 *
 * <pre>{@code
 * new SagaTestFixture<>(TransferSaga.class)
 *         .given(new TransferRequested(transferId, a, b, 60))
 *         .whenPublishing(new Withdrawn(a, 60, transferId))
 *         .expectDispatchedCommands(new Deposit(b, 60, transferId))
 *         .expectSaga(transferId, s -> assertEquals(PENDING, s.status));
 * }</pre>
 *
 * <p>Events run through the real machinery — {@code InMemoryEventBus}
 * registration, the {@link SagaResolver}'s correlation index ({@code @SagaStart}
 * creation included), saga persistence to an {@code InMemorySagaStore} after
 * each handler — but synchronously on the caller's thread, via the same
 * single-segment resolve path the cluster pump uses. Commands the saga sends
 * through {@code GlobalRegistry.get(CommandBus.class)} are captured by a
 * recording bus instead of being handled; {@code given} events' commands are
 * discarded, {@code whenPublishing}'s are what the expectations see.
 *
 * <p>A saga handler that throws fails the test immediately (in production the
 * group's DLQ policy would absorb it).
 *
 * <p>Like {@link AggregateTestFixture}, the constructor owns the global
 * {@link GlobalRegistry}: one fixture at a time, one fixture per test.
 */
public class SagaTestFixture<T> {

    private final Class<T> sagaClass;
    private final JacksonMessageSerializer serializer;
    private final SagaStore sagaStore;
    private final FixtureBuses.RecordingCommandBus commandBus;
    private final SagaResolver resolver;

    private Throwable handlerError;
    private boolean whenCalled;

    public SagaTestFixture(Class<T> sagaClass) {
        Saga ann = sagaClass.getAnnotation(Saga.class);
        if (ann == null) {
            throw new IllegalArgumentException(sagaClass.getName() + " is not annotated with @Saga");
        }
        this.sagaClass = sagaClass;
        String group = ann.group();

        GlobalRegistry.clear();
        CommandForwarding.reset();
        SegmentCalculator.setSegments(1);
        serializer = new JacksonMessageSerializer();
        GlobalRegistry.register(MessageSerializer.class, serializer);
        GlobalRegistry.register(UpcastersManager.class, new UpcastersManager(serializer, List.of()));

        sagaStore = new InMemorySagaStore();
        GlobalRegistry.register(SagaStore.class, sagaStore);
        commandBus = new FixtureBuses.RecordingCommandBus(serializer);
        GlobalRegistry.register(CommandBus.class, commandBus);
        InMemoryEventBus eventBus = new InMemoryEventBus(serializer, sagaStore, new InMemoryDlqStore());
        // In production a failed saga handler goes to the group's DLQ policy; in a
        // unit fixture it must fail the test. The policy callback is the hook: it
        // sees the raw handler exception before any DLQ routing.
        eventBus.setProcessingGroupPolicy(new Bus.ProcessingGroupPolicyConfig(group,
                new DlqEnqueuePolicy() {
                    @Override
                    public DlqEnqueueDecisionResult shouldEnqueue(InternalMessage message, Throwable error) {
                        handlerError = error;
                        return DlqEnqueueDecisionResult.ignore();
                    }
                }, new NullSequencePolicy()));
        GlobalRegistry.register(EventBus.class, eventBus);
        GlobalRegistry.register(sagaClass);

        // Pull mode: build the saga lanes + resolver without threads, then drive
        // resolveForSegment synchronously — the cluster pump's own dispatch path.
        eventBus.getHandler().setPullMode(true);
        eventBus.start();
        resolver = eventBus.getHandler().resolver(group);
        if (resolver == null) {
            throw new IllegalStateException("Saga " + sagaClass.getName()
                    + " produced no resolver for group '" + group + "' — no @SagaHandler methods?");
        }
    }

    /** Deliver prior events; commands they make the saga dispatch are discarded. */
    public SagaTestFixture<T> given(Object... events) {
        if (whenCalled) {
            throw new IllegalStateException("given(...) must be called before whenPublishing(...)");
        }
        for (Object event : events) {
            deliver(event);
        }
        commandBus.reset();
        return this;
    }

    /** Publish the event under test; commands dispatched from here on are recorded. */
    public SagaTestFixture<T> whenPublishing(Object event) {
        if (whenCalled) {
            throw new IllegalStateException(
                    "whenPublishing(...) may be called once per fixture; create a new fixture per scenario");
        }
        whenCalled = true;
        deliver(event);
        return this;
    }

    /** Assert the saga dispatched exactly these commands, in order (structural JSON equality). */
    public SagaTestFixture<T> expectDispatchedCommands(Object... expected) {
        requireWhen();
        List<Object> actual = commandBus.recorded();
        if (actual.size() != expected.length) {
            throw new AssertionError("Expected " + expected.length + " dispatched command(s) "
                    + describe(List.of(expected)) + " but got " + actual.size() + " " + describe(actual));
        }
        for (int i = 0; i < expected.length; i++) {
            if (!expected[i].getClass().equals(actual.get(i).getClass())) {
                throw new AssertionError("Command #" + i + ": expected "
                        + expected[i].getClass().getSimpleName() + " but was "
                        + actual.get(i).getClass().getSimpleName());
            }
            JsonNode expectedNode = serializer.deserializeToIntermediate(serializer.serialize(expected[i]));
            JsonNode actualNode = serializer.deserializeToIntermediate(serializer.serialize(actual.get(i)));
            if (!expectedNode.equals(actualNode)) {
                throw new AssertionError("Command #" + i + " ("
                        + expected[i].getClass().getSimpleName() + ") differs:\n"
                        + "  expected: " + expectedNode + "\n  actual:   " + actualNode);
            }
        }
        return this;
    }

    /** Assert the published event made the saga dispatch nothing. */
    public SagaTestFixture<T> expectNoDispatchedCommands() {
        requireWhen();
        List<Object> actual = commandBus.recorded();
        if (!actual.isEmpty()) {
            throw new AssertionError("Expected no dispatched commands but got " + describe(actual));
        }
        return this;
    }

    /** Load the saga by its correlation value and run the assertions on its persisted state. */
    public SagaTestFixture<T> expectSaga(Object correlationValue, Consumer<T> assertions) {
        var instance = sagaStore.loadSagaByCorrelationId(
                String.valueOf(correlationValue), sagaClass.getSimpleName());
        if (instance.isEmpty()) {
            throw new AssertionError("No " + sagaClass.getSimpleName()
                    + " correlated to '" + correlationValue + "' in the saga store");
        }
        assertions.accept(serializer.deserialize(instance.get().getContent(), sagaClass));
        return this;
    }

    /** Assert no saga is correlated to the value (never started, or ended and deleted). */
    public SagaTestFixture<T> expectNoSaga(Object correlationValue) {
        var instance = sagaStore.loadSagaByCorrelationId(
                String.valueOf(correlationValue), sagaClass.getSimpleName());
        if (instance.isPresent()) {
            throw new AssertionError("Expected no " + sagaClass.getSimpleName()
                    + " correlated to '" + correlationValue + "' but one is stored");
        }
        return this;
    }

    // ── internals ────────────────────────────────────────────────────────────

    private void deliver(Object event) {
        Event ann = event.getClass().getAnnotation(Event.class);
        if (ann == null) {
            throw new IllegalArgumentException(
                    "Published payload " + event.getClass().getName() + " is not annotated with @Event");
        }
        UUID aggregateId = Bus.extractAggregateId(event);
        if (aggregateId == null) {
            throw new IllegalArgumentException("Event " + event.getClass().getName()
                    + " carries no @AggregateIdentifier — @SagaStart creation routes by it");
        }
        Context ctx = new Context();
        ctx.setType(event.getClass().getSimpleName());
        ctx.setVersion(ann.version());
        ctx.setAggregateId(aggregateId);
        InternalMessage msg = new InternalMessage();
        msg.setEvent(true);
        msg.setContext(ctx);
        msg.setPayload(serializer.serialize(event));
        resolver.resolveForSegment(msg, 0); // 1 segment → every saga and event maps here
        if (handlerError != null) {
            Throwable error = handlerError;
            handlerError = null;
            throw new AssertionError("Saga handler failed for "
                    + event.getClass().getSimpleName() + ": " + error.getMessage(), error);
        }
    }

    private void requireWhen() {
        if (!whenCalled) {
            throw new IllegalStateException("Call whenPublishing(event) before asserting expectations");
        }
    }

    private String describe(List<Object> items) {
        List<String> names = new ArrayList<>();
        for (Object o : items) names.add(o.getClass().getSimpleName());
        return names.toString();
    }
}
