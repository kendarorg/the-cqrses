package org.kendar.cqrses.pg;

import org.kendar.cqrses.bus.Bus;
import org.kendar.cqrses.bus.Context;
import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.dlq.*;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.utils.TriConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Shared fakes for {@link ProcessingGroup} / {@link ProcessingGroupsManager}
 * unit tests. Hand-written (no Mockito) so the dispatch / DLQ branching is
 * exercised against real behaviour rather than stub matchers.
 */
final class PgTestKit {

    private PgTestKit() {
    }

    /**
     * A concrete {@link Bus} whose {@code findTarget} / {@code getMessageClass}
     * are supplied per-test. Extends Bus directly so it is NOT a CommandBus —
     * {@code ProcessingGroupsManager} therefore treats it as the event side.
     */
    static class TestBus extends Bus {
        BiFunction<Object, Registration, Object> findTargetFn = (m, r) -> m;
        Function<String, Class<?>> messageClassFn = name -> Object.class;

        TestBus(MessageSerializer serializer) {
            super(serializer);
        }

        @Override
        public Object findTarget(Object message, Registration registration) {
            return findTargetFn.apply(message, registration);
        }

        @Override
        public Class<?> getMessageClass(String name) {
            return messageClassFn.apply(name);
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
        protected boolean registerInternal(Class<?> handlerClass) {
            return false;
        }
    }

    /** Serializer that returns a preset deserialized object regardless of bytes. */
    static class FixedSerializer implements MessageSerializer<Object, Object> {
        private final Object deserialized;

        FixedSerializer(Object deserialized) {
            this.deserialized = deserialized;
        }

        @Override
        public byte[] serialize(Object domainObject) {
            return new byte[0];
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T deserialize(byte[] payload, Class<T> targetClass) {
            return (T) deserialized;
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

    /** Serializer that always throws on deserialize, to drive the catch path. */
    static class ThrowingSerializer implements MessageSerializer<Object, Object> {
        @Override
        public byte[] serialize(Object domainObject) {
            return new byte[0];
        }

        @Override
        public <T> T deserialize(byte[] payload, Class<T> targetClass) {
            throw new RuntimeException("deserialize boom");
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

    /** Records every interaction so tests can assert DLQ behaviour. */
    static class RecordingDlqStore implements DlqStore {
        boolean blocked = false;
        final List<DlqItem> added = new ArrayList<>();
        final List<String> evicted = new ArrayList<>();
        final List<String> blockedChecks = new ArrayList<>();
        private final ConcurrentHashMap<String, ConcurrentLinkedQueue<DlqItem>> items = new ConcurrentHashMap<>();

        @Override
        public boolean hasBlockedItems(String sequenceId) {
            blockedChecks.add(sequenceId);
            return blocked;
        }

        @Override
        public void evictFirst(String sequenceId) {
            evicted.add(sequenceId);
            var q = items.get(sequenceId);
            if (q != null) q.poll();
        }

        @Override
        public void addItem(DlqItem item, String sequenceId) {
            added.add(item);
            items.computeIfAbsent(sequenceId, k -> new ConcurrentLinkedQueue<>()).add(item);
        }

        @Override
        public List<DlqItem> listItems(String sequenceId) {
            var q = items.get(sequenceId);
            return q == null ? List.of() : q.stream().toList();
        }

        @Override
        public Optional<DlqItem> getItem(UUID id) {
            return added.stream().filter(i -> id.equals(i.getId())).findFirst();
        }

        @Override
        public void updateStatus(UUID id, DlqItemStatus status) {
            getItem(id).ifPresent(i -> i.setStatus(status));
        }

        @Override
        public void updateItem(DlqItem item) {
        }

        @Override
        public void removeItem(UUID id) {
            getItem(id).ifPresent(item -> {
                var q = items.get(item.getSequenceId());
                if (q != null) q.remove(item);
            });
        }

        @Override
        public void clear() {
            added.clear();
            evicted.clear();
            items.clear();
        }
    }

    /** {@link DlqEnqueuePolicy} returning a fixed decision and recording the error. */
    static class FixedEnqueuePolicy extends DlqEnqueuePolicy {
        private final DlqEnqueueDecisionResult result;
        final AtomicReference<Throwable> lastError = new AtomicReference<>();
        int calls = 0;

        FixedEnqueuePolicy(DlqEnqueueDecisionResult result) {
            this.result = result;
        }

        @Override
        public DlqEnqueueDecisionResult shouldEnqueue(InternalMessage message, Throwable error) {
            calls++;
            lastError.set(error);
            return result;
        }
    }

    static Bus.ProcessingGroupPolicyConfig policy(String group,
                                                  DlqEnqueueDecisionResult decision,
                                                  String sequenceId) {
        return new Bus.ProcessingGroupPolicyConfig(
                group,
                new FixedEnqueuePolicy(decision),
                eventCommand -> sequenceId);
    }

    static Bus.Registration registration(Class<?> handlerClass,
                                         TriConsumer<Object, Object, Context> body) {
        return new Bus.Registration(handlerClass, (t, m, c) -> {
            body.accept(t, m, c);
            return null;
        }, Bus.defaultProcessingGroupPolicyConfig(), null);
    }

    static InternalMessage message(String type, UUID aggregateId) {
        var ctx = new Context();
        ctx.setType(type);
        ctx.setAggregateId(aggregateId);
        var m = new InternalMessage();
        m.setContext(ctx);
        m.setPayload(new byte[0]);
        return m;
    }
}
