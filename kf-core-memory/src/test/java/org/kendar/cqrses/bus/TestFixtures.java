package org.kendar.cqrses.bus;

import org.kendar.cqrses.annotations.*;
import org.kendar.cqrses.repositories.AggregateSnapshot;
import org.kendar.cqrses.repositories.EventStore;
import org.kendar.cqrses.repositories.SagaInstance;
import org.kendar.cqrses.repositories.SagaStore;
import org.kendar.cqrses.serialization.MessageSerializer;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Shared test annotations, commands, events, aggregates, sagas.
 */
final class TestFixtures {

    private TestFixtures() {
    }

    /**
     * Wrap a payload in an InternalMessage. Stores the FQN class name in
     * Context.type so the test-side override can recover the class.
     */
    static InternalMessage wrap(Object payload) {
        InternalMessage m = new InternalMessage();
        Context c = new Context();
        c.setType(payload.getClass().getName());
        m.setContext(c);
        m.setPayload(new byte[0]);
        return m;
    }

    @Command(version = 1)
    public static class CreateThing {
        @AggregateIdentifier
        public UUID id;
        public String value;

        public CreateThing() {
        }

        public CreateThing(UUID id, String value) {
            this.id = id;
            this.value = value;
        }
    }

    @Event(version = 1)
    public static class ThingCreated {
        @AggregateIdentifier
        public UUID id;
        public String value;

        public ThingCreated() {
        }

        public ThingCreated(UUID id, String value) {
            this.id = id;
            this.value = value;
        }
    }

    @Aggregate(group = "things")
    public static class ThingAggregate {
        public final List<CreateThing> received = new CopyOnWriteArrayList<>();

        public void handle(CreateThing cmd) {
            received.add(cmd);
        }
    }

    @Projection(group = "things-proj")
    public static class ThingProjection {
        public final List<ThingCreated> seen = new CopyOnWriteArrayList<>();

        public void on(ThingCreated event) {
            seen.add(event);
        }
    }

    @Saga(group = "things-saga")
    public static class ThingSaga {
        public final List<ThingCreated> seen = new CopyOnWriteArrayList<>();

        public void on(ThingCreated event) {
            seen.add(event);
        }
    }

    /**
     * Minimal in-memory EventStore stub — only enough for InMemoryCommandBus construction.
     */
    public static class StubEventStore implements EventStore {
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
        public <T> Optional<T> loadAggregate(UUID aggregateId, Class<T> result) {
            return Optional.empty();
        }
    }

    /**
     * Minimal in-memory SagaStore stub — only enough for InMemoryEventBus construction.
     */
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

    /**
     * Tracks what was deserialized so tests can verify dispatch reached the handler.
     */
    public static class RecordingSerializer implements MessageSerializer<Object, Object> {
        public final java.util.List<byte[]> serializeCalls = new CopyOnWriteArrayList<>();
        public final java.util.List<Class<?>> deserializeCalls = new CopyOnWriteArrayList<>();
        private final MessageSerializer<?,?> delegate;

        public RecordingSerializer(MessageSerializer delegate) {
            this.delegate = delegate;
        }

        @Override
        public byte[] serialize(Object o) {
            byte[] b = delegate.serialize(o);
            serializeCalls.add(b);
            return b;
        }

        @Override
        public <T> T deserialize(byte[] payload, Class<T> targetClass) {
            deserializeCalls.add(targetClass);
            return delegate.deserialize(payload, targetClass);
        }

        @Override
        public Object serializeToFormat(Object domainObject) {
            return domainObject;
        }

        @Override
        public <T> T deserializeFromFormat(Object payload, Class<T> targetClass) {
            return (T)payload;
        }

        @Override
        public Object deserializeToFormat(byte[] payload) {
            return payload;
        }

        @Override
        public Object deserializeToIntermediate(byte[] payload) {
            return payload;
        }
    }
}
