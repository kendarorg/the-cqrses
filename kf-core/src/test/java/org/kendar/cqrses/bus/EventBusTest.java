package org.kendar.cqrses.bus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.annotations.Aggregate;
import org.kendar.cqrses.annotations.AggregateIdentifier;
import org.kendar.cqrses.annotations.Event;
import org.kendar.cqrses.annotations.Projection;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.repositories.SagaInstance;
import org.kendar.cqrses.repositories.SagaStore;
import org.kendar.cqrses.serialization.JacksonMessageSerializer;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.utils.UUIDGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class EventBusTest {

    private CapturingEventBus bus;
    private StubSagaStore sagaStore;
    private JacksonMessageSerializer serializer;

    @BeforeEach
    void setUp() {
        GlobalRegistry.clear();
        sagaStore = new StubSagaStore();
        serializer = new JacksonMessageSerializer();
        bus = new CapturingEventBus(serializer, sagaStore);
    }

    @AfterEach
    void tearDown() {
        GlobalRegistry.clear();
    }

    @Test
    void constructorStoresSerializerAndSagaStore() {
        assertSame(serializer, bus.serializer);
        assertSame(sagaStore, bus.sagaStore);
    }

    @Test
    void sendBuildsContextFromEventAnnotation() {
        UUID id = UUIDGenerator.newUuid();
        VersionedEvent e = new VersionedEvent();
        e.id = id;
        bus.send(e);

        Context ctx = bus.contexts.get(0);
        assertEquals(id, ctx.getAggregateId());
        assertEquals(-1L, ctx.getAggregateVersion());
        assertEquals("VersionedEvent", ctx.getType());
        assertEquals(5L, ctx.getVersion());
        assertNotNull(ctx.getTraceId());
    }

    @Test
    void sendWithExplicitAggregateVersionPropagatesIt() {
        VersionedEvent e = new VersionedEvent();
        e.id = UUIDGenerator.newUuid();
        bus.send(e, 8);
        assertEquals(8L, bus.contexts.get(0).getAggregateVersion());
    }

    @Test
    void sendDefaultEventVersionAnnotationReadsAsOne() {
        DefaultVersionEvent e = new DefaultVersionEvent();
        e.id = UUIDGenerator.newUuid();
        bus.send(e);
        assertEquals(1L, bus.contexts.get(0).getVersion());
    }

    @Test
    void sendForwardsEventInstanceUnchanged() {
        VersionedEvent e = new VersionedEvent();
        e.id = UUIDGenerator.newUuid();
        bus.send(e);
        assertSame(e, bus.events.get(0));
    }

    @Test
    void sendGivesUniqueTraceIdPerCall() {
        VersionedEvent e = new VersionedEvent();
        e.id = UUIDGenerator.newUuid();
        bus.send(e);
        bus.send(e);
        assertNotEquals(bus.contexts.get(0).getTraceId(),
                bus.contexts.get(1).getTraceId());
    }

    @Test
    void registerInternalReturnsFalseForPlainClass() {
        bus.register(PlainClass.class);
        assertFalse(bus.subscribedClasses.contains(PlainClass.class));
    }

    @Test
    void registerInternalReturnsFalseForAggregateClass() {
        bus.register(AggregateClass.class);
        assertFalse(bus.subscribedClasses.contains(AggregateClass.class));
    }

    @Test
    void registerInternalReturnsTrueForProjectionClass() {
        bus.register(ProjectionClass.class);
        assertTrue(bus.subscribedClasses.contains(ProjectionClass.class));
    }

    @Test
    void registerInternalReturnsTrueForSagaClass() {
        bus.register(SagaClass.class);
        assertTrue(bus.subscribedClasses.contains(SagaClass.class));
    }

    @Test
    void subscribeProjectionWithoutExplicitPolicyDoesNotThrow() {
        // Group "no-policy-g" is never configured via setProcessingGroupPolicy. Before
        // grill item 6 this NPE'd in storeMethod (bare processingGroupPolicies.get);
        // resolvePolicyConfig now falls back to a default config carrying the group.
        bus.register(ProjectionWithHandler.class);
        assertTrue(bus.subscribedClasses.contains(ProjectionWithHandler.class));
    }

    @Test
    void duplicateEventSimpleNameRejected() {
        bus.register(ProjectionHandlingDup1.class);
        // Holder2.Dup shares the simple name "Dup" (case-insensitively "DUP") with
        // Holder1.Dup → grill item 4 fails fast instead of silently dropping it.
        assertThrows(org.kendar.cqrses.exceptions.InvalidRegistrationException.class,
                () -> bus.register(ProjectionHandlingDup2.class));
    }

    @Test
    void loadSagaInstanceStubReturnsNull() throws Exception {
        VersionedEvent e = new VersionedEvent();
        e.id = UUIDGenerator.newUuid();
        var method = Object.class.getDeclaredMethod("toString");
        var registration = new Bus.Registration(SagaClass.class, null, null, method);
        assertNull(bus.loadSagaInstance(e, registration));
    }

    // --- @SagaId type validation --------------------------------------------------

    @Test
    void sagaWithUuidSagaIdRegisters() {
        bus.register(SagaWithUuidId.class);
        assertTrue(bus.subscribedClasses.contains(SagaWithUuidId.class));
    }

    @Test
    void sagaWithNonUuidSagaIdThrowsAtRegistration() {
        var thrown = assertThrows(org.kendar.cqrses.exceptions.InvalidRegistrationException.class,
                () -> bus.register(SagaWithStringId.class));
        assertTrue(thrown.getMessage().contains("java.lang.String"));
        assertTrue(thrown.getMessage().contains("transferId"));
    }

    @Test
    void sagaWithoutSagaIdStillRegisters() {
        // Store-less fixture sagas (e.g. kf-core-memory's ThingSaga) have no @SagaId;
        // absence keeps failing at store time, not at registration.
        bus.register(SagaClass.class);
        assertTrue(bus.subscribedClasses.contains(SagaClass.class));
    }

    @org.kendar.cqrses.annotations.Saga(group = "saga-uuid-g")
    public static class SagaWithUuidId {
        @org.kendar.cqrses.annotations.SagaId
        public UUID transferId;
    }

    @org.kendar.cqrses.annotations.Saga(group = "saga-str-g")
    public static class SagaWithStringId {
        @org.kendar.cqrses.annotations.SagaId
        public String transferId;
    }

    @Event(version = 5)
    public static class VersionedEvent {
        @AggregateIdentifier
        public UUID id;
    }

    @Event
    public static class DefaultVersionEvent {
        @AggregateIdentifier
        public UUID id;
    }

    @Projection(group = "proj-g")
    public static class ProjectionClass {
    }

    @Projection(group = "no-policy-g")
    public static class ProjectionWithHandler {
        @org.kendar.cqrses.annotations.EventHandler
        public void on(VersionedEvent e) {
        }
    }

    static class Holder1 {
        @Event
        public static class Dup {
            @AggregateIdentifier
            public UUID id;
        }
    }

    static class Holder2 {
        @Event
        public static class DUP {
            @AggregateIdentifier
            public UUID id;
        }
    }

    @Projection(group = "dup-g1")
    public static class ProjectionHandlingDup1 {
        @org.kendar.cqrses.annotations.EventHandler
        public void on(Holder1.Dup e) {
        }
    }

    @Projection(group = "dup-g2")
    public static class ProjectionHandlingDup2 {
        @org.kendar.cqrses.annotations.EventHandler
        public void on(Holder2.DUP e) {
        }
    }

    // --- registerInternal -------------------------------------------------------

    @org.kendar.cqrses.annotations.Saga(group = "saga-g")
    public static class SagaClass {
    }

    @Aggregate
    public static class AggregateClass {
    }

    public static class PlainClass {
    }

    public static class StubSagaStore implements SagaStore {
        @Override
        public void storeSaga(Object saga) {
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

    public static class CapturingEventBus extends EventBus {
        public final List<Context> contexts = new ArrayList<>();
        public final List<Object> events = new ArrayList<>();

        public CapturingEventBus(MessageSerializer s, SagaStore ss) {
            super(s, ss);
        }

        @Override
        public Object findTarget(Object e, Registration registration) {
            return null;
        }

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        public void clear() {
        }

        @Override
        void send(Object command, Context context) {
            events.add(command);
            contexts.add(context);
        }
    }
}
