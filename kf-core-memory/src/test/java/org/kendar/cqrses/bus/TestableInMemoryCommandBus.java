package org.kendar.cqrses.bus;

import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.di.TargetType;
import org.kendar.cqrses.dlq.DlqStore;
import org.kendar.cqrses.repositories.EventStore;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.utils.TriConsumer;

import java.lang.reflect.Method;

/**
 * Test-only InMemoryCommandBus that overrides {@code rehydrateAggregate} so
 * findTarget()/dispatch can return a real instance instead of null, and
 * exposes {@code storeMethod} so tests can register handlers without going
 * through the annotation-driven subscribe path.
 */
public class TestableInMemoryCommandBus extends InMemoryCommandBus {

    private final java.util.Map<Class<?>, Object> aggregatesByCommandType = new java.util.HashMap<>();

    public TestableInMemoryCommandBus(MessageSerializer serializer, EventStore eventStore, DlqStore dlqStore) {
        super(serializer, eventStore,dlqStore);
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

    /**
     * Wire a command type → aggregate instance so {@link #rehydrateAggregate} can return it.
     */
    public void primeAggregate(Class<?> commandType, Object aggregateInstance) {
        aggregatesByCommandType.put(commandType, aggregateInstance);
    }

    @Override
    protected Object rehydrateAggregate(Object command, Bus.Registration targetType) {
        // Return whichever aggregate the test wired for this command's type.
        if (command instanceof InternalMessage im) {
            String type = im.getContext().getType();
            Class<?> cls;
            try {
                cls = Class.forName(type);
            } catch (ClassNotFoundException e) {
                cls = getMessageClass(type);
            }
            return cls == null ? null : aggregatesByCommandType.get(cls);
        }
        return aggregatesByCommandType.get(command.getClass());
    }

    /**
     * Test-only handler registration: skips the annotation-driven subscribe path.
     */
    public void registerHandler(Class<?> aggregateClass,
                                Class<?> commandType,
                                String processingGroup,
                                TriConsumer<Object, Object, Context> handler, Method methodInfo) {
        // Mark the aggregate class as an AGGREGATE target so findTarget() routes correctly.
        // We don't call GlobalRegistry.register(class, instance) here because that would
        // trigger autoSubscribe(), which dereferences the bus slot in the registry.
        forceTargetType(aggregateClass, TargetType.AGGREGATE);
        storeMethod(aggregateClass, commandType,
                Bus.defaultProcessingGroupPolicyConfig(),
                (t, m, c) -> {
                    handler.accept(t, m, c);
                    return null;
                }, methodInfo);
    }
}
