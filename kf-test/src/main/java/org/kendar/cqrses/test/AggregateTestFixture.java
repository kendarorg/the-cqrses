package org.kendar.cqrses.test;

import com.fasterxml.jackson.databind.JsonNode;
import org.kendar.cqrses.annotations.Aggregate;
import org.kendar.cqrses.annotations.Event;
import org.kendar.cqrses.bus.Bus;
import org.kendar.cqrses.bus.CommandBus;
import org.kendar.cqrses.bus.Context;
import org.kendar.cqrses.bus.EventBus;
import org.kendar.cqrses.bus.FixtureBuses;
import org.kendar.cqrses.bus.InMemoryCommandBus;
import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.cluster.spi.CommandForwarding;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.pg.SegmentCalculator;
import org.kendar.cqrses.repositories.EventStore;
import org.kendar.cqrses.repositories.InMemoryEventStore;
import org.kendar.cqrses.repositories.InMemoryDlqStore;
import org.kendar.cqrses.serialization.JacksonMessageSerializer;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.serialization.Upcaster;
import org.kendar.cqrses.serialization.UpcastersManager;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Given–when–then unit-test fixture for one aggregate, in the style of Axon's
 * {@code AggregateTestFixture}:
 *
 * <pre>{@code
 * new AggregateTestFixture<>(AccountAggregate.class)
 *         .given(new AccountOpened(id), new Deposited(id, 100, null))
 *         .when(new Withdraw(id, 60, null))
 *         .expectEvents(new Withdrawn(id, 60, null))
 *         .expectState(a -> assertEquals(40, a.balance));
 * }</pre>
 *
 * <p>It is not a mock harness: the command runs through the real pipeline —
 * {@code InMemoryCommandBus} dispatch, rehydration from an
 * {@code InMemoryEventStore} (snapshots, upcasters and {@code @AggregateVersion}
 * included), the {@code EventApplyer} buffer, OCC version assignment on append,
 * and the {@code snapshotEvery} trigger — all synchronously on the caller's
 * thread, with no lane threads (the dispatch lanes are built in pull mode).
 *
 * <p>The fixture owns the {@link GlobalRegistry} (a process-wide locator): its
 * constructor clears and re-populates it, so build at most one fixture at a time
 * and never share one across concurrently-running tests. Each test method should
 * create its own fixture; {@code when} may be called once per fixture.
 *
 * <p>Expectations throw plain {@link AssertionError}s, so the fixture works under
 * any test framework. Event and result equality is structural — compared as
 * serialized JSON trees — so domain classes need no {@code equals}.
 */
public class AggregateTestFixture<T> {

    private final Class<T> aggregateClass;
    private final JacksonMessageSerializer serializer;
    private final InMemoryEventStore eventStore;
    private final InMemoryCommandBus commandBus;

    private UUID aggregateId;
    private int givenCount;
    private boolean whenCalled;
    private Object result;
    private Throwable thrown;

    public AggregateTestFixture(Class<T> aggregateClass, Upcaster... upcasters) {
        if (aggregateClass.getAnnotation(Aggregate.class) == null) {
            throw new IllegalArgumentException(
                    aggregateClass.getName() + " is not annotated with @Aggregate");
        }
        this.aggregateClass = aggregateClass;

        GlobalRegistry.clear();
        CommandForwarding.reset();
        SegmentCalculator.setSegments(1);
        serializer = new JacksonMessageSerializer();
        GlobalRegistry.register(MessageSerializer.class, serializer);
        GlobalRegistry.register(UpcastersManager.class,
                new UpcastersManager(serializer, List.of(upcasters)));

        eventStore = new InMemoryEventStore();
        GlobalRegistry.register(EventStore.class, eventStore);
        commandBus = new InMemoryCommandBus(serializer, eventStore, new InMemoryDlqStore());
        GlobalRegistry.register(CommandBus.class, commandBus);
        GlobalRegistry.register(EventBus.class, FixtureBuses.noopEventBus(serializer, null));
        GlobalRegistry.register(aggregateClass);

        // Build the dispatch lanes without spawning their pump threads: the only
        // path the fixture uses is the fully synchronous sendSync, so the fixture
        // leaks no threads and needs no teardown.
        commandBus.getHandler().setPullMode(true);
        commandBus.start();
    }

    /** Seed the aggregate's history. Events are appended exactly as the pipeline would store them. */
    public AggregateTestFixture<T> given(Object... events) {
        if (whenCalled) {
            throw new IllegalStateException("given(...) must be called before when(...)");
        }
        for (Object event : events) {
            InternalMessage msg = envelope(event);
            if (aggregateId == null) aggregateId = msg.getContext().getAggregateId();
            eventStore.appendEvents(List.of(msg));
            givenCount++;
        }
        return this;
    }

    /** Dispatch the command under test through the real synchronous pipeline. Once per fixture. */
    public AggregateTestFixture<T> when(Object command) {
        if (whenCalled) {
            throw new IllegalStateException(
                    "when(...) may be called once per fixture; create a new fixture per scenario");
        }
        whenCalled = true;
        if (commandBus.getMessageClass(command.getClass().getSimpleName()) == null) {
            throw new IllegalStateException("Aggregate " + aggregateClass.getName()
                    + " has no @CommandHandler for " + command.getClass().getName());
        }
        UUID commandAggregate = Bus.extractAggregateId(command);
        if (commandAggregate != null) aggregateId = commandAggregate;
        try {
            result = commandBus.sendSync(command);
        } catch (Throwable t) {
            thrown = t;
        }
        return this;
    }

    /** Assert the command emitted exactly these events, in order (structural JSON equality). */
    public AggregateTestFixture<T> expectEvents(Object... expected) {
        requireWhen();
        requireNoFailure();
        List<InternalMessage> actual = emitted();
        if (actual.size() != expected.length) {
            throw new AssertionError("Expected " + expected.length + " event(s) "
                    + describe(List.of(expected)) + " but " + actual.size()
                    + " were emitted " + describeMessages(actual));
        }
        for (int i = 0; i < expected.length; i++) {
            String expectedType = expected[i].getClass().getSimpleName();
            String actualType = actual.get(i).getContext().getType();
            if (!expectedType.equals(actualType)) {
                throw new AssertionError("Event #" + i + ": expected type " + expectedType
                        + " but was " + actualType);
            }
            JsonNode expectedNode = serializer.deserializeToIntermediate(serializer.serialize(expected[i]));
            JsonNode actualNode = serializer.deserializeToIntermediate(actual.get(i).getPayload());
            if (!expectedNode.equals(actualNode)) {
                throw new AssertionError("Event #" + i + " (" + expectedType + ") differs:\n"
                        + "  expected: " + expectedNode + "\n  actual:   " + actualNode);
            }
        }
        return this;
    }

    /** Assert the command emitted no events at all. */
    public AggregateTestFixture<T> expectNoEvents() {
        requireWhen();
        requireNoFailure();
        List<InternalMessage> actual = emitted();
        if (!actual.isEmpty()) {
            throw new AssertionError("Expected no events but " + actual.size()
                    + " were emitted " + describeMessages(actual));
        }
        return this;
    }

    /** Assert the command failed with (a subtype of) the given exception. */
    public AggregateTestFixture<T> expectException(Class<? extends Throwable> type) {
        requireWhen();
        if (thrown == null) {
            throw new AssertionError("Expected " + type.getName()
                    + " but the command succeeded with result " + result);
        }
        if (!type.isInstance(thrown)) {
            throw new AssertionError("Expected " + type.getName()
                    + " but the command failed with " + thrown.getClass().getName(), thrown);
        }
        return this;
    }

    /**
     * Assert the {@code @CommandHandler}'s return value: {@code equals} first,
     * falling back to structural JSON equality for result types without one.
     */
    public AggregateTestFixture<T> expectResult(Object expected) {
        requireWhen();
        requireNoFailure();
        if (expected == null ? result == null : expected.equals(result)) return this;
        if (expected != null && result != null) {
            JsonNode expectedNode = serializer.deserializeToIntermediate(serializer.serialize(expected));
            JsonNode actualNode = serializer.deserializeToIntermediate(serializer.serialize(result));
            if (expectedNode.equals(actualNode)) return this;
        }
        throw new AssertionError("Expected result " + expected + " but was " + result);
    }

    /** Rehydrate the aggregate from the store (given + emitted events) and run the assertions. */
    public AggregateTestFixture<T> expectState(Consumer<T> assertions) {
        requireWhen();
        requireNoFailure();
        if (aggregateId == null) {
            throw new IllegalStateException(
                    "No aggregate id: neither the given events nor the command carry @AggregateIdentifier");
        }
        T instance = eventStore.loadAggregate(aggregateId, aggregateClass)
                .orElseThrow(() -> new AssertionError("Aggregate " + aggregateId + " could not be rehydrated"));
        assertions.accept(instance);
        return this;
    }

    /** The handler's raw return value (after {@code when}). */
    @SuppressWarnings("unchecked")
    public <R> R getResult() {
        requireWhen();
        requireNoFailure();
        return (R) result;
    }

    /** The event store backing the fixture, for snapshot/stream assertions. */
    public EventStore getEventStore() {
        return eventStore;
    }

    // ── internals ────────────────────────────────────────────────────────────

    private InternalMessage envelope(Object event) {
        Event ann = event.getClass().getAnnotation(Event.class);
        if (ann == null) {
            throw new IllegalArgumentException(
                    "Given payload " + event.getClass().getName() + " is not annotated with @Event");
        }
        UUID id = Bus.extractAggregateId(event);
        if (id == null) id = aggregateId;
        if (id == null) {
            throw new IllegalArgumentException("Given event " + event.getClass().getName()
                    + " carries no @AggregateIdentifier field");
        }
        Context ctx = new Context();
        ctx.setType(event.getClass().getSimpleName());
        ctx.setVersion(ann.version());
        ctx.setAggregateId(id);
        ctx.setAggregateVersion(-1); // store assigns 0..n-1, exactly like the pipeline
        InternalMessage msg = new InternalMessage();
        msg.setEvent(true);
        msg.setContext(ctx);
        msg.setPayload(serializer.serialize(event));
        return msg;
    }

    /** Everything appended after the given events — segment 0 is the whole store with 1 segment. */
    private List<InternalMessage> emitted() {
        return eventStore.loadSegmentTail(0, givenCount - 1L, Integer.MAX_VALUE);
    }

    private void requireWhen() {
        if (!whenCalled) {
            throw new IllegalStateException("Call when(command) before asserting expectations");
        }
    }

    private void requireNoFailure() {
        if (thrown != null) {
            throw new AssertionError("Command failed with "
                    + thrown.getClass().getName() + ": " + thrown.getMessage()
                    + " (use expectException(...) if the failure is the expectation)", thrown);
        }
    }

    private String describe(List<Object> events) {
        List<String> names = new ArrayList<>();
        for (Object e : events) names.add(e.getClass().getSimpleName());
        return names.toString();
    }

    private String describeMessages(List<InternalMessage> messages) {
        List<String> names = new ArrayList<>();
        for (InternalMessage m : messages) names.add(m.getContext().getType());
        return names.toString();
    }
}
