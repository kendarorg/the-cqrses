package org.kendar.cqrses.bus;

import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.di.TargetType;
import org.kendar.cqrses.dlq.DlqStore;
import org.kendar.cqrses.repositories.SagaStore;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.utils.TriConsumer;

/**
 * Test-only InMemoryEventBus that overrides {@code loadSagaInstance} so
 * findTarget()/dispatch can return a real saga instance, and exposes
 * {@code storeMethod} so tests can register handlers without going through
 * the annotation-driven subscribe path.
 */
public class TestableInMemoryEventBus extends InMemoryEventBus {

    private final java.util.Map<Class<?>, Object> sagasByEventType = new java.util.HashMap<>();

    public TestableInMemoryEventBus(MessageSerializer serializer, SagaStore sagaStore, DlqStore dlqStore) {
        super(serializer, sagaStore,dlqStore);
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

    public void primeSaga(Class<?> eventType, Object sagaInstance) {
        sagasByEventType.put(eventType, sagaInstance);
    }

    @Override
    protected Object loadSagaInstance(Object event, Bus.Registration consumer) {
        if (event instanceof InternalMessage im) {
            String type = im.getContext().getType();
            Class<?> cls;
            try {
                cls = Class.forName(type);
            } catch (ClassNotFoundException e) {
                cls = getMessageClass(type);
            }
            return cls == null ? null : sagasByEventType.get(cls);
        }
        return sagasByEventType.get(event.getClass());
    }

    public void registerHandler(Class<?> handlerClass,
                                Class<?> eventType,
                                TargetType handlerKind,
                                String processingGroup,
                                TriConsumer<Object, Object, Context> handler) {
        forceTargetType(handlerClass, handlerKind);
        storeMethod(handlerClass, eventType,
                Bus.defaultProcessingGroupPolicyConfig(),
                (t, m, c) -> {
                    handler.accept(t, m, c);
                    return null;
                }, null);
    }
}
