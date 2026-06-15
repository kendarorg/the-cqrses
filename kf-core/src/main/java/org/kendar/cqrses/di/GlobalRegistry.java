package org.kendar.cqrses.di;

import org.kendar.cqrses.annotations.*;
import org.kendar.cqrses.bus.CommandBus;
import org.kendar.cqrses.bus.Context;
import org.kendar.cqrses.bus.EventBus;
import org.kendar.cqrses.exceptions.InvalidRegistrationException;
import org.kendar.cqrses.utils.TriConsumer;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static org.kendar.cqrses.utils.ReflectionUtils.*;

public class GlobalRegistry {


    private static final ConcurrentHashMap<Class<?>, Object> registry = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Class<?>, List<?>> multiRegistry = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Class<?>, TargetType> classRegistry = new ConcurrentHashMap<>();
    // aggregateClass -> (eventClass -> applier). Exactly one applier per pair; duplicates throw.
    private static final ConcurrentHashMap<Class<?>, Map<Class<?>, TriConsumer<Object, Object, Context>>>
            aggregateEventHandlers = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Class<?>, Field> commandAggregateIdCache = new ConcurrentHashMap<>();
    private static volatile Function<Class<?>, Object> fallbackResolver = null;
    // One-way setup→runtime freeze latch (grill item 1). Flipped true by start(),
    // reset ONLY by clear() (full teardown). stop() does NOT reset it: a cluster-part
    // restart bounces the pump, never re-subscribes, so the topology stays frozen and
    // live across it.
    private static volatile boolean started = false;
    // Case-folded simple name -> handler class, for @Aggregate / @Saga collision
    // detection (grill item 4). Simple-name identity (the saga-store key + upcaster
    // index) requires names to be unique case-insensitively.
    private static final ConcurrentHashMap<String, Class<?>> handlerSimpleNames = new ConcurrentHashMap<>();

    private GlobalRegistry() {
    }

    public static void start(){
        GlobalRegistry.get(CommandBus.class).start();
        GlobalRegistry.get(EventBus.class).start();
        // Latch the topology closed: from here on subscribe / register / policy /
        // setSegments fail-fast instead of racing an in-flight publish.
        started = true;
    }

    /** True once {@link #start()} has run (until the next {@link #clear()}). */
    public static boolean isStarted() {
        return started;
    }

    /**
     * Setup→runtime freeze guard. Topology-mutating calls (subscribe, register,
     * {@code setProcessingGroupPolicy}, {@code SegmentCalculator.setSegments}) call
     * this so a mutation after start surfaces as a loud, located
     * {@link IllegalStateException} instead of the silent publish-vs-subscribe data
     * race CLAUDE.md documents as "undefined behaviour".
     */
    public static void assertNotStarted(String operation) {
        if (started) {
            throw new IllegalStateException(
                    "Topology change '" + operation + "' attempted after the framework started. "
                            + "Handler subscriptions, processing-group policies and the segment count "
                            + "are frozen once running; stop() + clear() to begin a fresh setup phase.");
        }
    }

    /**
     * Reject a second {@link TargetType#AGGREGATE} / {@link TargetType#SAGA} type
     * whose simple name collides (case-insensitively) with one already registered.
     * Simple-name identity is intentional (eases upcasting, see grill item 3) but it
     * means two same-named classes in different packages would silently fight over
     * the saga-store correlation key and the upcaster index — this protects the very
     * mechanism that identity relies on.
     */
    private static void checkSimpleNameCollision(Class<?> type, TargetType targetType) {
        if (targetType != TargetType.AGGREGATE && targetType != TargetType.SAGA) return;
        var folded = type.getSimpleName().toLowerCase(Locale.ROOT);
        var previous = handlerSimpleNames.putIfAbsent(folded, type);
        if (previous != null && previous != type) {
            throw new InvalidRegistrationException(
                    "Duplicate aggregate/saga simple name '" + type.getSimpleName()
                            + "' (case-insensitive): " + previous.getName() + " and " + type.getName()
                            + ". Simple-class-name identity (saga-store key + upcaster index) requires "
                            + "unique names across @Aggregate / @Saga types.");
        }
    }
    public static void stop(){
        GlobalRegistry.get(CommandBus.class).stop();
        GlobalRegistry.get(EventBus.class).stop();
    }

    public static TargetType getTargetType(Class<?> toFInd) {
        return classRegistry.get(toFInd);
    }

    public static void register(Class<?> type) {
        var targetType = TargetType.NONE;
        if (hasClassAnnotation(type, CommandInterceptor.class)) targetType = TargetType.COMMAND_HANDLER;
        if (hasClassAnnotation(type, Projection.class)) targetType = TargetType.PROJECTION;
        if (hasClassAnnotation(type, Aggregate.class)) targetType = TargetType.AGGREGATE;
        if (hasClassAnnotation(type, Saga.class)) targetType = TargetType.SAGA;

        checkSimpleNameCollision(type, targetType);
        Object previous = classRegistry.put(type, targetType);
        // Double registration of the same key is a no-op for auto-subscribe:
        // the bus already has the handler; re-running subscribe would either
        // throw "already subscribed" or pointlessly walk the annotations again.
        if (previous == null) {
            // Validate the snapshot contract before any subscription happens, so a
            // broken aggregate never becomes partially registered on the buses.
            if (targetType == TargetType.AGGREGATE) {
                checkSnapshotContract(type);
            }
            autoSubscribe(type);
            if (targetType == TargetType.AGGREGATE) {
                registerAggregateEventHandlers(type);
            }
        }
    }

    /**
     * Fail-fast for {@code @Aggregate(snapshotEvery = N)}: the automatic trigger
     * needs {@code getSnapshot()} to produce the payload and rehydration needs
     * {@code setSnapshot(T)} to apply it, so a missing half must surface at
     * registration (setup phase), not as a skipped snapshot or a
     * MissingAggregateSnapshotHandler at the first rehydrate in production.
     */
    private static void checkSnapshotContract(Class<?> type) {
        Aggregate ann = type.getAnnotation(Aggregate.class);
        if (ann == null || ann.snapshotEvery() <= 0) return;
        Method getter;
        try {
            getter = type.getMethod("getSnapshot");
        } catch (NoSuchMethodException e) {
            throw new InvalidRegistrationException(
                    "Aggregate " + type.getName() + " declares snapshotEvery=" + ann.snapshotEvery()
                            + " but has no public getSnapshot() method");
        }
        if (getter.getReturnType() == void.class) {
            throw new InvalidRegistrationException(
                    "Aggregate " + type.getName() + " declares snapshotEvery=" + ann.snapshotEvery()
                            + " but getSnapshot() returns void");
        }
        boolean hasSetter = false;
        for (Method m : type.getMethods()) {
            if (m.getName().equals("setSnapshot") && m.getParameterCount() == 1) {
                hasSetter = true;
                break;
            }
        }
        if (!hasSetter) {
            throw new InvalidRegistrationException(
                    "Aggregate " + type.getName() + " declares snapshotEvery=" + ann.snapshotEvery()
                            + " but has no setSnapshot(<SnapshotType>) method to rehydrate from");
        }
    }

    private static void registerAggregateEventHandlers(Class<?> type) {
        for (Method method : getMethodsAnnotatedWith(type, EventHandler.class)) {
            Class<?>[] classParams = method.getParameterTypes();
            if (classParams.length == 0) continue;
            Class<?> eventType = classParams[0];
            if (!hasClassAnnotation(eventType, Event.class)) {
                throw new InvalidRegistrationException(
                        "Event " + eventType.getName() + " targeted by aggregate " +
                                type.getName() + "#" + method.getName() +
                                " must be annotated with @Event");
            }
            TriConsumer<Object, Object, Context> applier = (aggregateInstance, event, context) -> {
                Object[] params = new Object[classParams.length];
                params[0] = event;
                for (int i = 1; i < classParams.length; i++) {
                    if (classParams[i] == Context.class) {
                        params[i] = context;
                    } else {
                        params[i] = GlobalRegistry.get(classParams[i]);
                    }
                }
                try {
                    method.invoke(aggregateInstance, params);
                } catch (InvocationTargetException ite) {
                    Throwable cause = ite.getCause();
                    if (cause instanceof RuntimeException re) throw re;
                    if (cause instanceof Error err) throw err;
                    throw new RuntimeException(cause);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            };
            Map<Class<?>, TriConsumer<Object, Object, Context>> perAggregate =
                    aggregateEventHandlers.computeIfAbsent(type, k -> new ConcurrentHashMap<>());
            TriConsumer<Object, Object, Context> previous = perAggregate.putIfAbsent(eventType, applier);
            if (previous != null) {
                throw new InvalidRegistrationException(
                        "DUPLICATE @EventHandler on aggregate " + type.getName() +
                                ": event " + eventType.getName() +
                                " is already handled by another method in the same class (or parent). " +
                                "Offending method: " + method + ". " +
                                "Each aggregate must declare AT MOST ONE @EventHandler per event type.");
            }
        }
    }

    /**
     * Returns the event applier registered for {@code event.getClass()} on {@code aggregateClass}, or null.
     */
    public static TriConsumer<Object, Object, Context> getEventHandler(Class<?> aggregateClass, Object event) {
        return getEventHandler(aggregateClass, event.getClass());
    }

    /**
     * Returns the event applier registered for {@code eventClass} on {@code aggregateClass}, or null.
     */
    public static TriConsumer<Object, Object, Context> getEventHandler(Class<?> aggregateClass, Class<?> eventClass) {
        var perAggregate = aggregateEventHandlers.get(aggregateClass);
        if (perAggregate == null) return null;
        return perAggregate.get(eventClass);
    }

    /**
     * Returns an unmodifiable view of every event applier registered for
     * {@code aggregateClass} keyed by event {@code Class<?>}. Empty map if the
     * aggregate isn't registered or declares no @EventHandler methods. Used by
     * {@link org.kendar.cqrses.repositories.BaseEventStore} to fold stored
     * events onto a fresh aggregate during rehydration.
     */
    public static Map<Class<?>, TriConsumer<Object, Object, Context>> getAggregateEventHandlers(Class<?> aggregateClass) {
        var perAggregate = aggregateEventHandlers.get(aggregateClass);
        if (perAggregate == null) return Collections.emptyMap();
        return Collections.unmodifiableMap(perAggregate);
    }

    public static void register(Class<?> type, Object instance) {
        var targetType = TargetType.NONE;
        if (hasClassAnnotation(type, CommandInterceptor.class)) targetType = TargetType.COMMAND_HANDLER;
        if (hasClassAnnotation(type, Projection.class)) targetType = TargetType.PROJECTION;
        if (hasClassAnnotation(type, Aggregate.class)) targetType = TargetType.AGGREGATE;
        if (hasClassAnnotation(type, Saga.class)) targetType = TargetType.SAGA;
        checkSimpleNameCollision(type, targetType);
        // classRegistry drives EventBus/CommandBus.findTarget routing; storing
        // the targetType here is what makes a register(class, instance) of a
        // @Projection actually resolve as a PROJECTION at dispatch time.
        classRegistry.putIfAbsent(type, targetType);
        if(instance!=null) {
            Object previous = registry.put(type, instance);
            // Besides the explicit key, make the instance resolvable under every
            // type in its hierarchy (superclasses + interfaces) so callers can
            // get() it by any supertype, not just the key it was registered
            // under. putIfAbsent: an explicit registration of a supertype always
            // wins over this derived one, and the first instance registered for a
            // shared interface wins over later ones.
            for (Class<?> supertype : getAllSupertypes(instance.getClass())) {
                if (supertype == type) continue;
                registry.putIfAbsent(supertype, instance);
            }
            // Double registration of the same key is a no-op for auto-subscribe:
            // the bus already has the handler; re-running subscribe would either
            // throw "already subscribed" or pointlessly walk the annotations again.
            if (previous == null) {
                autoSubscribe(type);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T get(Class<T> type) {
        Object found = registry.get(type);
        if (found != null) return (T) found;
        Function<Class<?>, Object> resolver = fallbackResolver;
        if (resolver == null) return null;
        Object resolved = resolver.apply(type);
        if (resolved == null) return null;
        // putIfAbsent: another caller may have raced us; first writer wins.
        Object existing = registry.putIfAbsent(type, resolved);
        return (T) (existing != null ? existing : resolved);
    }

    public static <T> Optional<T> find(Class<T> type) {
        return Optional.ofNullable(get(type));
    }

    /**
     * Register a list of instances under {@code toRegister}. Unlike {@link #register},
     * this keeps every instance addressable as a group rather than collapsing to a
     * single binding — repeated calls for the same key append to the existing list.
     */
    public static void registerMulti(Class<?> toRegister, List<Object> instances) {
        multiRegistry.compute(toRegister, (k, existing) -> {
            var merged = new ArrayList<Object>();
            if (existing != null) merged.addAll(existing);
            merged.addAll(instances);
            return Collections.unmodifiableList(merged);
        });
    }

    /**
     * Returns the list registered under {@code type} via {@link #registerMulti},
     * or an empty list if none. The returned list is unmodifiable.
     */
    @SuppressWarnings("unchecked")
    public static <T> List<T> getMulti(Class<T> type) {
        List<?> found = multiRegistry.get(type);
        if (found == null) return Collections.emptyList();
        return (List<T>) found;
    }

    /**
     * Returns a snapshot of every registered instance, regardless of its key
     * type. Used by {@code Framework.stop()} to find buses / stores by
     * {@code instanceof} when callers may have registered under either the
     * interface or the implementation class.
     */
    public static Collection<Object> allInstances() {
        // One instance is stored under several keys (its whole type hierarchy),
        // so dedupe by identity: each distinct instance appears exactly once,
        // which is what the instanceof-walk in Framework.stop() expects.
        Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        var out = new ArrayList<>();
        for (Object v : registry.values()) {
            if (seen.add(v)) out.add(v);
        }
        return Collections.unmodifiableCollection(out);
    }

    /**
     * Install (or clear, with {@code null}) a fallback resolver for misses
     * on {@link #get}. The resolver should return {@code null} for unknown
     * types; non-null results are cached back into the registry.
     */
    public static void setFallbackResolver(Function<Class<?>, Object> resolver) {
        fallbackResolver = resolver;
    }

    // ── Auto-subscribe ────────────────────────────────────────────────────────

    /**
     * Removes all registrations.
     *
     * <p>Also drops the {@link #fallbackResolver}: it captures the wiring context (e.g. the Spring
     * {@code ApplicationContext} in {@code KfBootstrap.start}), so a stale resolver surviving a clear
     * would fire {@code get()} misses against a torn-down context — throwing
     * {@code "...has been closed already"} on the next setup phase in the same JVM (the symptom that
     * leaked across {@code ApplicationContextRunner} runs).
     */
    public static void clear() {
        registry.clear();
        multiRegistry.clear();
        classRegistry.clear();
        aggregateEventHandlers.clear();
        commandAggregateIdCache.clear();
        handlerSimpleNames.clear();
        fallbackResolver = null;
        // Reopen the setup phase: a full teardown is the only thing that resets the
        // freeze latch (grill item 1). This is the test/full-restart path.
        started = false;
    }

    private static void autoSubscribe(Class<?> type) {
        boolean cmd = isCommandSide(type);
        boolean evt = isEventSide(type);
        if (!cmd && !evt) return;

        // Best-effort: if no bus is registered in GlobalRegistry the caller is
        // wiring things up manually (typical for unit tests that instantiate a
        // bare LocalCommandBus / LocalEventBus). Skip silently — the caller
        // will call bus.subscribe(type) themselves.
        if (cmd) {
            var cb = (CommandBus) registry.get(CommandBus.class);
            cb.register(type);
        }
        if (evt) {
            var eb = (EventBus) registry.get(EventBus.class);
            eb.register(type);
        }
    }

    public static Field getFieldAnnotatedWith(Class<?> clazz, Class<? extends Annotation> annotation) {
        if (commandAggregateIdCache.containsKey(clazz)) {
            var result = commandAggregateIdCache.get(clazz);
            if (result == NULL_FIELD) return null;
            return result;
        }
        var result = Arrays.stream(clazz.getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(annotation))
                .findFirst();
        if (result.isPresent()) {
            var field = result.get();
            field.setAccessible(true);
            commandAggregateIdCache.put(clazz, field);
        } else {
            commandAggregateIdCache.put(clazz, NULL_FIELD);
        }
        return getFieldAnnotatedWith(clazz, annotation);
    }
}
