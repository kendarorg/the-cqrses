package org.kendar.cqrses.bus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.di.TargetType;
import org.kendar.cqrses.exceptions.InvalidHandlerException;
import org.kendar.cqrses.repositories.InMemoryDlqStore;
import org.kendar.cqrses.serialization.JacksonMessageSerializer;
import org.kendar.cqrses.utils.UUIDGenerator;

import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryEventBusTest {

    private TestableInMemoryEventBus bus;
    private TestFixtures.RecordingSerializer recording;

    private static Bus.Registration registration(Class<?> handlerClass) {
        return new Bus.Registration(handlerClass, null, null, noAnnotationMethod());
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
        bus = new TestableInMemoryEventBus(recording, new TestFixtures.StubSagaStore(),new InMemoryDlqStore());
        GlobalRegistry.register(org.kendar.cqrses.bus.EventBus.class, bus);
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
    void findTargetForProjectionReturnsRegisteredInstance() {
        TestFixtures.ThingProjection projection = new TestFixtures.ThingProjection();
        GlobalRegistry.register(TestFixtures.ThingProjection.class, projection);
        forceTargetType(TestFixtures.ThingProjection.class, TargetType.PROJECTION);

        assertSame(projection,
                bus.findTarget(new TestFixtures.ThingCreated(UUIDGenerator.newUuid(), "v"),
                        registration(TestFixtures.ThingProjection.class)));
    }

    @Test
    void findTargetForSagaInvokesLoadSagaInstance() {
        TestFixtures.ThingSaga saga = new TestFixtures.ThingSaga();
        bus.primeSaga(TestFixtures.ThingCreated.class, saga);
        forceTargetType(TestFixtures.ThingSaga.class, TargetType.SAGA);

        assertSame(saga,
                bus.findTarget(new TestFixtures.ThingCreated(UUIDGenerator.newUuid(), "v"),
                        registration(TestFixtures.ThingSaga.class)));
    }

    @Test
    void findTargetForUnsupportedTargetTypeThrows() {
        forceTargetType(TestFixtures.ThingAggregate.class, TargetType.AGGREGATE);
        assertThrows(InvalidHandlerException.class, () ->
                bus.findTarget(new TestFixtures.ThingCreated(UUIDGenerator.newUuid(), "v"),
                        registration(TestFixtures.ThingAggregate.class)));
    }

    @Test
    void asyncSendDispatchesToProjectionHandlerEventually() throws Exception {
        CopyOnWriteArrayList<TestFixtures.ThingCreated> seen = new CopyOnWriteArrayList<>();
        TestFixtures.ThingProjection proj = new TestFixtures.ThingProjection();
        GlobalRegistry.register(TestFixtures.ThingProjection.class, proj);
        bus.registerHandler(TestFixtures.ThingProjection.class, TestFixtures.ThingCreated.class,
                TargetType.PROJECTION, "proj-g",
                (target, message, ctx) -> seen.add((TestFixtures.ThingCreated) message));
        bus.start();

        UUID id = UUIDGenerator.newUuid();
        bus.send(new TestFixtures.ThingCreated(id, "v"));

        long deadline = System.currentTimeMillis() + 2_000;
        while (seen.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(1, seen.size());
        assertEquals(id, seen.get(0).id);
    }

    @Test
    void sendBuildsContextWithEventVersionAndType() {
        java.util.List<Context> contexts = new CopyOnWriteArrayList<>();
        TestFixtures.ThingProjection proj = new TestFixtures.ThingProjection();
        GlobalRegistry.register(TestFixtures.ThingProjection.class, proj);
        bus.registerHandler(TestFixtures.ThingProjection.class, TestFixtures.ThingCreated.class,
                TargetType.PROJECTION, "proj-g",
                (t, m, ctx) -> contexts.add(ctx));
        bus.start();

        UUID id = UUIDGenerator.newUuid();
        bus.send(new TestFixtures.ThingCreated(id, "v"));

        long deadline = System.currentTimeMillis() + 2_000;
        while (contexts.isEmpty() && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {
            }
        }
        assertEquals(1, contexts.size());
        Context ctx = contexts.get(0);
        assertEquals(id, ctx.getAggregateId());
        assertEquals("ThingCreated", ctx.getType());
        assertEquals(1L, ctx.getVersion());
        assertNotNull(ctx.getTraceId());
    }

    @Test
    void loadSagaInstanceInBaseClassReturnsNull() {
        // Documents the unimplemented behaviour of the base hook.
        InMemoryEventBus plain = new InMemoryEventBus(new JacksonMessageSerializer(),
                new TestFixtures.StubSagaStore(),new InMemoryDlqStore());
        forceTargetType(TestFixtures.ThingSaga.class, TargetType.SAGA);
        assertNull(plain.findTarget(new TestFixtures.ThingCreated(UUIDGenerator.newUuid(), "v"),
                registration(TestFixtures.ThingSaga.class)));
    }

    @Test
    void clearDoesNotThrowEvenBeforeStart() {
        assertDoesNotThrow(bus::clear);
    }

    @Test
    void stopBeforeStartDoesNotThrow() {
        assertDoesNotThrow(bus::stop);
    }
}
