package org.kendar.cqrses.repositories;

import org.kendar.cqrses.annotations.Aggregate;
import org.kendar.cqrses.annotations.AggregateVersion;
import org.kendar.cqrses.bus.Context;
import org.kendar.cqrses.bus.EventApplyer;
import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.exceptions.MissingAggregateConstructorException;
import org.kendar.cqrses.exceptions.MissingAggregateSnapshotHandlerException;
import org.kendar.cqrses.observability.Observability;
import org.kendar.cqrses.observability.TraceRecorder;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.serialization.UpcastersManager;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public abstract class BaseEventStore implements EventStore {

    private static final org.slf4j.Logger LOGGER =
            org.slf4j.LoggerFactory.getLogger(BaseEventStore.class);

    public static final String DEFAULT_GROUP = "default";

    public static String groupOf(Class<?> type) {
        Aggregate ann = type.getAnnotation(Aggregate.class);
        return ann == null ? DEFAULT_GROUP : ann.group();
    }

    private static final Method NO_SETTER;
    private static final ConcurrentHashMap<Class<?>, Method> snapshotSetterCache = new ConcurrentHashMap<>();
    private static final Field NO_VERSION_FIELD;
    private static final ConcurrentHashMap<Class<?>, Field> versionFieldCache = new ConcurrentHashMap<>();
    private final UpcastersManager upcastersManager;

    public BaseEventStore() {
        this.upcastersManager = GlobalRegistry.get(UpcastersManager.class);
    }

    static {
        try {
            NO_SETTER = BaseEventStore.class.getDeclaredMethod("findSnapshotSetter", Class.class);
        } catch (NoSuchMethodException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    static {
        try {
            NO_VERSION_FIELD = BaseEventStore.class.getDeclaredField("snapshotSetterCache");
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static Field findVersionField(Class<?> aggregateType) {
        Field cached = versionFieldCache.computeIfAbsent(aggregateType, t -> {
            for (Class<?> c = t; c != null && c != Object.class; c = c.getSuperclass()) {
                for (Field f : c.getDeclaredFields()) {
                    if (f.isAnnotationPresent(AggregateVersion.class)) {
                        f.setAccessible(true);
                        return f;
                    }
                }
            }
            return NO_VERSION_FIELD;
        });
        return cached == NO_VERSION_FIELD ? null : cached;
    }

    private static void writeVersion(Field field, Object instance, long value) {
        try {
            Class<?> type = field.getType();
            if (type == long.class || type == Long.class) {
                field.set(instance, value);
            } else if (type == int.class || type == Integer.class) {
                field.set(instance, (int) value);
            } else {
                field.set(instance, value);
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Cannot write @AggregateVersion field " + field, e);
        }
    }

    private static Method findSnapshotSetter(Class<?> aggregateType) {
        Method cached = snapshotSetterCache.computeIfAbsent(aggregateType, t -> {
            for (Method m : t.getMethods()) {
                if (m.getName().equals("setSnapshot") && m.getParameterCount() == 1) {
                    m.setAccessible(true);
                    return m;
                }
            }
            return NO_SETTER;
        });
        return cached == NO_SETTER ? null : cached;
    }

    /**
     * Bring a stored snapshot up to the aggregate's current schema revision
     * ({@code @Aggregate.version()}, default 1) before it is applied.
     *
     * <p>A snapshot stored at the current revision passes through untouched. An
     * older (or newer) one is run through the registered upcaster chain, keyed by
     * the stored snapshot payload's simple type name (falling back to the
     * {@code setSnapshot} parameter type for legacy rows that carry no type) and
     * the stored {@code schema_version}. If the chain lands exactly on the current
     * revision the rewritten payload is adopted; otherwise the snapshot is
     * <b>discarded</b> — snapshots are best-effort, so the aggregate silently
     * replays its full event stream and the next snapshot write heals the row.
     */
    private <T> Optional<AggregateSnapshot> upcastSnapshot(Optional<AggregateSnapshot> loaded,
                                                           Class<T> aggregateType) {
        if (loaded.isEmpty()) return loaded;
        Aggregate ann = aggregateType.getAnnotation(Aggregate.class);
        long expected = ann == null ? 1 : ann.version();
        AggregateSnapshot snap = loaded.get();
        if (snap.getSchemaVersion() == expected) return loaded;

        String typeName = snap.getSnapshotType();
        if (typeName == null) {
            Method setter = findSnapshotSetter(aggregateType);
            if (setter != null) typeName = setter.getParameterTypes()[0].getSimpleName();
        }
        if (typeName != null && upcastersManager != null) {
            Context ctx = new Context();
            ctx.setType(typeName);
            ctx.setVersion(snap.getSchemaVersion());
            ctx.setAggregateId(snap.getAggregateId());
            ctx.setAggregateVersion(snap.getAggregateVersion());
            InternalMessage msg = new InternalMessage();
            msg.setContext(ctx);
            msg.setPayload(snap.getSnapshot());
            msg = upcastersManager.upcast(msg);
            if (msg.getContext().getVersion() == expected) {
                snap.setSnapshot(msg.getPayload());
                snap.setSchemaVersion(expected);
                return Optional.of(snap);
            }
        }
        LOGGER.warn("Discarding snapshot for aggregate {} ({}): stored schema version {} cannot be "
                        + "upcast to current @Aggregate version {}; replaying the full event stream",
                snap.getAggregateId(), aggregateType.getSimpleName(), snap.getSchemaVersion(), expected);
        return Optional.empty();
    }

    @Override
    public <T> Optional<T> loadAggregate(UUID aggregateId, Class<T> aggregateType) {
        long t0 = System.nanoTime();
        int[] replayed = {0};
        try {
            return loadAggregateTimed(aggregateId, aggregateType, replayed);
        } finally {
            Observability.get().onAggregateRehydrated(
                    aggregateType.getSimpleName(), replayed[0], System.nanoTime() - t0);
        }
    }

    private <T> Optional<T> loadAggregateTimed(UUID aggregateId, Class<T> aggregateType, int[] replayed) {
        T instance;
        try {
            instance = aggregateType.getDeclaredConstructor().newInstance();
        } catch (Exception ex) {
            throw new MissingAggregateConstructorException(
                    "Cannot instantiate " + aggregateType.getName(), ex);
        }
        long mark = System.nanoTime();
        var aggregateSnapshot = upcastSnapshot(loadSnapshot(aggregateId), aggregateType);
        if (TraceRecorder.active()) {
            TraceRecorder.stage("rehydrate.snapshot", System.nanoTime() - mark,
                    aggregateSnapshot.isEmpty() ? 0 : 1);
            mark = System.nanoTime();
        }
        var lastSnapshottedEvent = aggregateSnapshot.isEmpty() ? -1 : aggregateSnapshot.get().getAggregateVersion();
        var events = loadEvents(aggregateId, lastSnapshottedEvent);
        replayed[0] = events == null ? 0 : events.size();
        if (TraceRecorder.active()) {
            TraceRecorder.stage("rehydrate.load", System.nanoTime() - mark, replayed[0]);
        }

        Field versionField = findVersionField(aggregateType);
        if (versionField != null) {
            long initial = aggregateSnapshot.isEmpty() ? 0L : aggregateSnapshot.get().getAggregateVersion();
            writeVersion(versionField, instance, initial);
        }

        if (!aggregateSnapshot.isEmpty()) {
            Method setter = findSnapshotSetter(aggregateType);
            if (setter == null) {
                throw new MissingAggregateSnapshotHandlerException(
                        "Aggregate " + aggregateType.getName() +
                                " has a stored snapshot but no setSnapshot(<SnapshotType>) method");
            }
            Class<?> snapshotType = setter.getParameterTypes()[0];
            Object snapshotValue = GlobalRegistry.get(MessageSerializer.class)
                    .deserialize(aggregateSnapshot.get().getSnapshot(), snapshotType);
            try {
                setter.invoke(instance, snapshotValue);
            } catch (Exception ite) {
                throw new MissingAggregateSnapshotHandlerException("Invalid setter for snapshot on " + aggregateType.getName());
            }
        }
        if (events == null || events.isEmpty()) {
            return Optional.of(instance);
        }

        var handlers = GlobalRegistry.getAggregateEventHandlers(aggregateType);
        if (handlers.isEmpty()) {
            // No registered @EventHandler methods → nothing to fold; return the
            // bare instance rather than silently dropping the load.
            return Optional.of(instance);
        }

        var serializer = GlobalRegistry.get(MessageSerializer.class);
        var ordered = new java.util.ArrayList<>(events);
        ordered.sort(java.util.Comparator.comparingLong(m -> m.getContext().getAggregateVersion()));

        long replayMark = System.nanoTime();
        for (InternalMessage msg : ordered) {
            msg = upcastersManager.upcast(msg);
            String typeName = msg.getContext().getType();
            Class<?> eventClass = handlers.keySet().stream()
                    .filter(c -> c.getSimpleName().equals(typeName))
                    .findFirst()
                    .orElse(null);
            if (eventClass == null) continue; // Aggregate doesn't handle this event type
            Object event = serializer.deserialize(msg.getPayload(), eventClass);
            // Rehydrate fold goes through the same entry point as the command
            // flow. No command buffer is active here, so apply() only folds —
            // it doesn't record. The stored event's context is forwarded so
            // @EventHandler methods that declare a Context parameter see the
            // persisted version + aggregateId.
            EventApplyer.apply(instance, event, msg.getContext());
            if (versionField != null) {
                writeVersion(versionField, instance, msg.getContext().getAggregateVersion());
            }
        }
        if (TraceRecorder.active()) {
            TraceRecorder.stage("rehydrate.replay", System.nanoTime() - replayMark, ordered.size());
        }
        return Optional.of(instance);
    }
}
