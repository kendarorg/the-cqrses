package org.kendar.cqrses.bus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.annotations.CommandHandler;
import org.kendar.cqrses.annotations.CreationPolicy;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.di.TargetType;
import org.kendar.cqrses.exceptions.InvalidHandlerException;
import org.kendar.cqrses.repositories.AggregateSnapshot;
import org.kendar.cqrses.repositories.EventStore;
import org.kendar.cqrses.repositories.InMemoryDlqStore;
import org.kendar.cqrses.serialization.JacksonMessageSerializer;
import org.kendar.cqrses.utils.UUIDGenerator;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryCommandBusTest {

    private TestableInMemoryCommandBus bus;
    private TestFixtures.RecordingSerializer recording;

    private static Method methodWithPolicy(String methodName) {
        try {
            return PolicyHolder.class.getMethod(methodName, TestFixtures.CreateThing.class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static Bus.Registration registration(Class<?> handlerClass) {
        return registration(handlerClass, noAnnotationMethod());
    }

    private static Bus.Registration registration(Class<?> handlerClass, Method methodInfo) {
        return new Bus.Registration(handlerClass, null, null, methodInfo);
    }

    private static java.lang.reflect.Method noAnnotationMethod() {
        try {
            return Object.class.getMethod("toString");
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static void forceTargetType(Class<?> type, TargetType targetType) {
        try {
            var f = GlobalRegistry.class.getDeclaredField("classRegistry");
            f.setAccessible(true);
            @SuppressWarnings("unchecked")
            var map = (java.util.concurrent.ConcurrentHashMap<Class<?>, TargetType>) f.get(null);
            map.put(type, targetType);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeEach
    void setUp() {
        GlobalRegistry.clear();
        recording = new TestFixtures.RecordingSerializer(new JacksonMessageSerializer());
        bus = new TestableInMemoryCommandBus(recording, new TestFixtures.StubEventStore(),new InMemoryDlqStore());
        // Register the bus so GlobalRegistry.autoSubscribe() finds it instead of NPEing.
        GlobalRegistry.register(org.kendar.cqrses.bus.CommandBus.class, bus);
    }

    @AfterEach
    void tearDown() {
        try {
            bus.stop();
        } catch (Exception ignored) {
        }
        GlobalRegistry.clear();
    }

    @Test
    void findTargetForAggregateInvokesRehydrateAggregate() {
        TestFixtures.ThingAggregate aggregate = new TestFixtures.ThingAggregate();
        bus.primeAggregate(TestFixtures.CreateThing.class, aggregate);
        forceTargetType(TestFixtures.ThingAggregate.class, TargetType.AGGREGATE);

        Object target = bus.findTarget(
                new TestFixtures.CreateThing(UUIDGenerator.newUuid(), "v"),
                registration(TestFixtures.ThingAggregate.class));
        assertSame(aggregate, target);
    }

    @Test
    void findTargetForCommandHandlerReturnsRegisteredInstance() {
        Object handler = new Object();
        GlobalRegistry.register(Object.class, handler);
        forceTargetType(Object.class, TargetType.COMMAND_HANDLER);

        assertSame(handler, bus.findTarget(new TestFixtures.CreateThing(UUIDGenerator.newUuid(), "v"), registration(Object.class)));
    }

    @Test
    void findTargetForUnsupportedTargetTypeThrows() {
        forceTargetType(TestFixtures.ThingProjection.class, TargetType.PROJECTION);
        assertThrows(InvalidHandlerException.class, () ->
                bus.findTarget(new TestFixtures.CreateThing(UUIDGenerator.newUuid(), "v"),
                        registration(TestFixtures.ThingProjection.class)));
    }

    @Test
    void sendSyncSerializesEnqueuesAndDispatchesToRegisteredHandler() {
        CopyOnWriteArrayList<Object> received = new CopyOnWriteArrayList<>();
        bus.primeAggregate(TestFixtures.CreateThing.class, new TestFixtures.ThingAggregate());
        bus.registerHandler(TestFixtures.ThingAggregate.class, TestFixtures.CreateThing.class, "g",
                (target, message, ctx) -> received.add(message), null);
        bus.start();

        TestFixtures.CreateThing cmd = new TestFixtures.CreateThing(UUIDGenerator.newUuid(), "hello");
        bus.sendSync(cmd);

        assertEquals(1, received.size());
        assertInstanceOf(TestFixtures.CreateThing.class, received.get(0));
        assertEquals("hello", ((TestFixtures.CreateThing) received.get(0)).value);
        assertEquals(1, recording.serializeCalls.size());
        assertTrue(recording.deserializeCalls.contains(TestFixtures.CreateThing.class));
    }

    @Test
    void asyncSendEnqueuesAndDispatchesEventually() throws Exception {
        CopyOnWriteArrayList<UUID> seen = new CopyOnWriteArrayList<>();
        bus.primeAggregate(TestFixtures.CreateThing.class, new TestFixtures.ThingAggregate());
        bus.registerHandler(TestFixtures.ThingAggregate.class, TestFixtures.CreateThing.class, "g",
                (target, message, ctx) -> seen.add(((TestFixtures.CreateThing) message).id), null);
        bus.start();

        UUID id = UUIDGenerator.newUuid();
        bus.send(new TestFixtures.CreateThing(id, "v"));

        long deadline = System.currentTimeMillis() + 2_000;
        while (seen.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(1, seen.size());
        assertEquals(id, seen.get(0));
    }

    @Test
    void sendBuildsContextWithAggregateIdAndCommandVersion() {
        java.util.List<Context> contexts = new CopyOnWriteArrayList<>();
        bus.primeAggregate(TestFixtures.CreateThing.class, new TestFixtures.ThingAggregate());
        bus.registerHandler(TestFixtures.ThingAggregate.class, TestFixtures.CreateThing.class, "g",
                (t, m, ctx) -> contexts.add(ctx), null);
        bus.start();

        UUID id = UUIDGenerator.newUuid();
        bus.sendSync(new TestFixtures.CreateThing(id, "v"));

        assertEquals(1, contexts.size());
        Context ctx = contexts.get(0);
        assertEquals(id, ctx.getAggregateId());
        assertEquals(-1L, ctx.getAggregateVersion());
        assertEquals("CreateThing", ctx.getType());
        assertEquals(1L, ctx.getVersion());
        assertNotNull(ctx.getTraceId());
    }

    @Test
    void sendWithExplicitAggregateVersionPropagatesIt() {
        java.util.List<Long> versions = new CopyOnWriteArrayList<>();
        bus.primeAggregate(TestFixtures.CreateThing.class, new TestFixtures.ThingAggregate());
        bus.registerHandler(TestFixtures.ThingAggregate.class, TestFixtures.CreateThing.class, "g",
                (t, m, ctx) -> versions.add(ctx.getAggregateVersion()), null);
        bus.start();

        bus.sendSync(new TestFixtures.CreateThing(UUIDGenerator.newUuid(), "v"), 5);
        assertEquals(java.util.List.of(5L), versions);
    }

    @Test
    void clearDoesNotThrowEvenBeforeStart() {
        assertDoesNotThrow(bus::clear);
    }

    @Test
    void stopBeforeStartDoesNotThrow() {
        assertDoesNotThrow(bus::stop);
    }

    @Test
    void rehydrateAggregateInBaseClassReturnsNull() {
        // Documents the unimplemented behaviour: the base InMemoryCommandBus
        // delegates to rehydrateAggregate, which is a stub returning null.
        InMemoryCommandBus plain = new InMemoryCommandBus(new JacksonMessageSerializer(),
                new TestFixtures.StubEventStore(),new InMemoryDlqStore());
        forceTargetType(TestFixtures.ThingAggregate.class, TargetType.AGGREGATE);
        assertNull(plain.findTarget(new TestFixtures.CreateThing(UUIDGenerator.newUuid(), "v"),
                registration(TestFixtures.ThingAggregate.class)));
    }

    @Test
    void rehydrateNeverCreate_existingAggregateIsReturned() {
        TestFixtures.ThingAggregate existing = new TestFixtures.ThingAggregate();
        InMemoryCommandBus plain = new InMemoryCommandBus(new JacksonMessageSerializer(),
                new AggregateReturningEventStore(existing),new InMemoryDlqStore());
        forceTargetType(TestFixtures.ThingAggregate.class, TargetType.AGGREGATE);

        Object target = plain.findTarget(
                new TestFixtures.CreateThing(UUIDGenerator.newUuid(), "v"),
                registration(TestFixtures.ThingAggregate.class, methodWithPolicy("never")));

        assertSame(existing, target);
    }

    @Test
    void rehydrateNeverCreate_missingAggregateReturnsNull() {
        InMemoryCommandBus plain = new InMemoryCommandBus(new JacksonMessageSerializer(),
                new TestFixtures.StubEventStore(),new InMemoryDlqStore());
        forceTargetType(TestFixtures.ThingAggregate.class, TargetType.AGGREGATE);

        Object target = plain.findTarget(
                new TestFixtures.CreateThing(UUIDGenerator.newUuid(), "v"),
                registration(TestFixtures.ThingAggregate.class, methodWithPolicy("never")));

        assertNull(target);
    }

    @Test
    void rehydrateCreateIfNotExists_emptyAggregateProducesFreshInstance() {
        InMemoryCommandBus plain = new InMemoryCommandBus(new JacksonMessageSerializer(),
                new TestFixtures.StubEventStore(),new InMemoryDlqStore());
        forceTargetType(TestFixtures.ThingAggregate.class, TargetType.AGGREGATE);

        Object target = plain.findTarget(
                new TestFixtures.CreateThing(UUIDGenerator.newUuid(), "v"),
                registration(TestFixtures.ThingAggregate.class, methodWithPolicy("ifNotExists")));

        assertNotNull(target);
        assertInstanceOf(TestFixtures.ThingAggregate.class, target);
        assertTrue(((TestFixtures.ThingAggregate) target).received.isEmpty());
    }

    @Test
    void rehydrateCreateIfNotExists_existingAggregateIsReused() {
        TestFixtures.ThingAggregate existing = new TestFixtures.ThingAggregate();
        InMemoryCommandBus plain = new InMemoryCommandBus(new JacksonMessageSerializer(),
                new AggregateReturningEventStore(existing),new InMemoryDlqStore());
        forceTargetType(TestFixtures.ThingAggregate.class, TargetType.AGGREGATE);

        Object target = plain.findTarget(
                new TestFixtures.CreateThing(UUIDGenerator.newUuid(), "v"),
                registration(TestFixtures.ThingAggregate.class, methodWithPolicy("ifNotExists")));

        assertSame(existing, target);
    }

    @Test
    void rehydrateAlwaysCreate_returnsFreshInstanceEvenIfStoreHasOne() {
        TestFixtures.ThingAggregate existing = new TestFixtures.ThingAggregate();
        InMemoryCommandBus plain = new InMemoryCommandBus(new JacksonMessageSerializer(),
                new AggregateReturningEventStore(existing),new InMemoryDlqStore());
        forceTargetType(TestFixtures.ThingAggregate.class, TargetType.AGGREGATE);

        Object target = plain.findTarget(
                new TestFixtures.CreateThing(UUIDGenerator.newUuid(), "v"),
                registration(TestFixtures.ThingAggregate.class, methodWithPolicy("always")));

        assertNotNull(target);
        assertNotSame(existing, target);
        assertInstanceOf(TestFixtures.ThingAggregate.class, target);
    }

    @Test
    void rehydrateAlwaysCreate_aggregateWithoutNoArgCtorThrowsInvalidHandler() {
        InMemoryCommandBus plain = new InMemoryCommandBus(new JacksonMessageSerializer(),
                new TestFixtures.StubEventStore(),new InMemoryDlqStore());
        forceTargetType(NoDefaultCtorAggregate.class, TargetType.AGGREGATE);

        assertThrows(InvalidHandlerException.class, () -> plain.findTarget(
                new TestFixtures.CreateThing(UUIDGenerator.newUuid(), "v"),
                registration(NoDefaultCtorAggregate.class, methodWithPolicy("always"))));
    }

    /**
     * Aggregate-shaped holder with no no-arg constructor — exercises the
     * catch branch in {@link CommandBus#rehydrateAggregate}.
     */
    static class NoDefaultCtorAggregate {
        NoDefaultCtorAggregate(String required) {
        }
    }

    /**
     * Holder whose methods carry @CommandHandler with each CreationPolicy.
     * Reflection picks them up via {@link #methodWithPolicy(String)}.
     */
    static class PolicyHolder {
        @CommandHandler(creationPolicy = CreationPolicy.NEVER_CREATE)
        public void never(TestFixtures.CreateThing cmd) {
        }

        @CommandHandler(creationPolicy = CreationPolicy.CREATE_IF_NOT_EXISTS)
        public void ifNotExists(TestFixtures.CreateThing cmd) {
        }

        @CommandHandler(creationPolicy = CreationPolicy.ALWAYS_CREATE)
        public void always(TestFixtures.CreateThing cmd) {
        }
    }

    /**
     * EventStore stub whose loadAggregate returns a fixed instance.
     */
    private static class AggregateReturningEventStore implements EventStore {
        private final Object instance;

        AggregateReturningEventStore(Object instance) {
            this.instance = instance;
        }

        @Override
        public void appendEvents(List<InternalMessage> events) {
        }

        @Override
        public List<InternalMessage> loadEvents(UUID aggregateId, long fromVersion) {
            return List.of();
        }

        @Override
        public Optional<AggregateSnapshot> loadSnapshot(UUID aggregateId) {
            return Optional.empty();
        }

        @Override
        public void storeSnapshot(UUID aggregateId, Object snapshotPayload, long schemaVersion, long aggregateVersion) {
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Optional<T> loadAggregate(UUID aggregateId, Class<T> result) {
            return Optional.of((T) instance);
        }
    }
}
