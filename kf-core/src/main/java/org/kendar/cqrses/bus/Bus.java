package org.kendar.cqrses.bus;

import org.kendar.cqrses.annotations.AggregateIdentifier;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.dlq.DlqEnqueueDecisionResult;
import org.kendar.cqrses.dlq.DlqEnqueuePolicy;
import org.kendar.cqrses.exceptions.InvalidAggregateId;
import org.kendar.cqrses.exceptions.InvalidRegistrationException;
import org.kendar.cqrses.pg.NullSequencePolicy;
import org.kendar.cqrses.pg.SequencePolicy;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.utils.TriFunction;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.kendar.cqrses.utils.ReflectionUtils.getMethodsAnnotatedWith;
import static org.kendar.cqrses.utils.ReflectionUtils.hasClassAnnotation;

public abstract class Bus {
    protected Map<String, ProcessingGroupPolicyConfig> processingGroupPolicies = new ConcurrentHashMap<>();
    protected final MessageSerializer serializer;
    protected Set<Class<?>> subscribedClasses = ConcurrentHashMap.newKeySet();
    //Processing group -> command type -> handler (target, method, policy)
    protected Map<String, Map<Class<?>, List<Registration>>> consumers = new ConcurrentHashMap<>();
    protected Map<Class<?>, Set<String>> eventsForProcessingGroups = new ConcurrentHashMap<>();
    protected Map<String, Class<?>> messageClasses = new ConcurrentHashMap<>();
    // Case-folded message simple name -> message class, for @Command/@Event
    // duplicate-name detection (grill item 4). Per-bus: a command and an event may
    // share a simple name, but two commands (or two events) may not.
    protected final Map<String, Class<?>> messageTypesByFoldedName = new ConcurrentHashMap<>();


    public Bus(MessageSerializer serializer) {
        this.serializer = serializer;
        // Constructor runs in the setup phase before start(); call the non-asserting
        // form so building a fresh bus never trips the freeze latch.
        applyProcessingGroupPolicy(defaultProcessingGroupPolicyConfig());
    }

    public void setProcessingGroupPolicy(ProcessingGroupPolicyConfig policyConfig) {
        GlobalRegistry.assertNotStarted("setProcessingGroupPolicy(" + policyConfig.processingGroup() + ")");
        applyProcessingGroupPolicy(policyConfig);
    }

    private void applyProcessingGroupPolicy(ProcessingGroupPolicyConfig policyConfig) {
        if (policyConfig.sequencePolicy() instanceof org.kendar.cqrses.pg.PerSegmentSequencePolicy ps) {
            ps.setGroup(policyConfig.processingGroup());
        }
        processingGroupPolicies.put(policyConfig.processingGroup, policyConfig);
    }

    /**
     * Resolve {@code group}'s policy for a subscription: the configured config if
     * present, else a default config <b>carrying {@code group}'s name</b>. One shared
     * resolver for both buses (grill item 6) — the event side previously did a bare
     * {@code processingGroupPolicies.get(group)} that NPE'd in {@code storeMethod}
     * for a group with no explicit policy.
     */
    protected ProcessingGroupPolicyConfig resolvePolicyConfig(String group) {
        var configured = processingGroupPolicies.get(group);
        return configured != null ? configured : defaultProcessingGroupPolicyConfig(group);
    }

    public static UUID extractAggregateId(Object command) {
        var field = GlobalRegistry.getFieldAnnotatedWith(command.getClass(), AggregateIdentifier.class);
        if (field != null) {
            try {
                return (UUID) field.get(command);
            } catch (Exception e) {
                throw new InvalidAggregateId("Cannot access aggregate id field " + field.getName() + " on command " + command.getClass().getName());
            }
        }
        return null;
    }

    public void register(Class<?> handlerClass) {
        // Implementation to register a handler class.
        // Re-subscribing an already-known class is an idempotent no-op (see the
        // double-registration note in GlobalRegistry) — allow it even after start;
        // only a genuinely new subscription trips the freeze latch (grill item 1).
        if (subscribedClasses.contains(handlerClass)) return;
        GlobalRegistry.assertNotStarted("subscribe(" + handlerClass.getSimpleName() + ")");
        if (registerInternal(handlerClass)) {
            subscribedClasses.add(handlerClass);
        }
    }

    protected ArrayList<Registration> analyzeMethods(Class<?> aggregateClass,
                                                     Class<? extends Annotation> methodAnnotation,
                                                     Class<? extends Annotation> firstParamAnnotation, ProcessingGroupPolicyConfig policyConfig) {
        Map<Class<?>, Method> seenByMessageType = new HashMap<>();
        var methods = new ArrayList<Bus.Registration>();
        for (Method method : getMethodsAnnotatedWith(aggregateClass, methodAnnotation)) {
            Class<?>[] params = method.getParameterTypes();
            if (params.length == 0) continue;
            Class<?> commandType = params[0];
            if (!hasClassAnnotation(commandType, firstParamAnnotation)) {
                throw new InvalidRegistrationException(
                        firstParamAnnotation.getSimpleName() + " " + commandType.getName() + " targeted by aggregate " +
                                aggregateClass.getName() + "#" + method.getName() +
                                " must be annotated with @" + firstParamAnnotation.getSimpleName());
            }
            Method previous = seenByMessageType.putIfAbsent(commandType, method);
            if (previous != null) {
                throw new InvalidRegistrationException(
                        "DUPLICATE @" + methodAnnotation.getSimpleName() + " on " + aggregateClass.getName() +
                                ": " + firstParamAnnotation.getSimpleName() + " " + commandType.getName() +
                                " is handled by BOTH " + previous + " AND " + method +
                                " (processing group '" + policyConfig.processingGroup() +
                                "'). Each class must declare AT MOST ONE @" +
                                methodAnnotation.getSimpleName() + " per " +
                                firstParamAnnotation.getSimpleName().toLowerCase() + " type.");
            }
            methods.add(registerMethod(aggregateClass, method, policyConfig));
        }
        return methods;
    }

    public ProcessingGroupPolicyConfig getProcessingGroupPolicy(String processingGroup) {
        if (processingGroupPolicies.containsKey(processingGroup)) return processingGroupPolicies.get(processingGroup);
        return processingGroupPolicies.get("default");
    }

    protected Registration registerMethod(Class<?> aggregateClass,
                                          Method method,
                                          ProcessingGroupPolicyConfig policyConfig) {

        var classParams = method.getParameterTypes();
        var commandType = classParams[0];
        TriFunction<Object, Object, Context, Object> invoker = (target, commandEvent, context) -> {
            Object[] params = new Object[classParams.length];
            params[0] = commandEvent;
            for (int i = 1; i < params.length; i++) {
                if (classParams[i] == Context.class) {
                    params[i] = context;
                } else {
                    params[i] = GlobalRegistry.get(classParams[i]);
                }
            }
            try {
                // Use the dispatcher-supplied target (the rehydrated aggregate
                // for AGGREGATE registrations; the singleton instance for
                // PROJECTION / interceptors). Pulling from GlobalRegistry here
                // would silently misroute aggregate commands to a singleton —
                // which is null when the class was registered without an
                // instance, blowing up on method.invoke.
                // The handler's return value flows back to the dispatcher: a
                // synchronous command surfaces it to the sender (sendSync),
                // event-side dispatch discards it.
                return method.invoke(target, params);
            } catch (java.lang.reflect.InvocationTargetException ite) {
                Throwable cause = ite.getCause();
                if (cause instanceof RuntimeException re) throw re;
                if (cause instanceof Error err) throw err;
                throw new RuntimeException(cause);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        };
        return storeMethod(aggregateClass, commandType, policyConfig, invoker, method);
    }

    public Class<?> getMessageClass(String name) {
        return messageClasses.get(name);
    }

    /**
     * Read-only view of the consumers registered for {@code group} (message type
     * &rarr; handler registrations), or {@code null} if no handler subscribes to
     * that group on this node. Exposes the already-frozen registration map so an
     * operator tool (e.g. {@code LocalDlqManager}) can re-invoke a group's
     * handlers without mutating the topology.
     */
    public Map<Class<?>, List<Registration>> getConsumers(String group) {
        return consumers.get(group);
    }

    /** The serializer this bus was built with (payload (de)serialization for retries). */
    public MessageSerializer getSerializer() {
        return serializer;
    }

    /**
     * Resolve the handler instance / aggregate that the given deserialised
     * message should be dispatched to. Callers (e.g.
     * {@link InternalMessage} first, since target resolution (aggregate-id
     * extraction, saga correlation) operates on domain-object fields.
     */
    public abstract Object findTarget(Object message, Registration registration);

    protected Registration storeMethod(
            Class<?> aggregateClass,
            Class<?> commandType,
            ProcessingGroupPolicyConfig policyConfig,
            TriFunction<Object, Object, Context, Object> invoker, Method methodInfo) {
        // Fail-fast on a case-insensitive simple-name collision between two distinct
        // message types (grill item 4). Without this the second class is silently
        // dropped (containsKey keeps the first), corrupting replay + upcaster routing,
        // which both key off the simple name.
        var foldedName = commandType.getSimpleName().toLowerCase(java.util.Locale.ROOT);
        var previousByName = messageTypesByFoldedName.putIfAbsent(foldedName, commandType);
        if (previousByName != null && previousByName != commandType) {
            throw new InvalidRegistrationException(
                    "Duplicate message simple name '" + commandType.getSimpleName()
                            + "' (case-insensitive): " + previousByName.getName() + " and "
                            + commandType.getName() + ". @Command/@Event identity is by simple name; "
                            + "names must be unique to keep replay and upcasting correct.");
        }
        if (!messageClasses.containsKey(commandType.getSimpleName())) {
            messageClasses.put(commandType.getSimpleName(), commandType);
        }
        if (!eventsForProcessingGroups.containsKey(commandType)) {
            eventsForProcessingGroups.put(commandType, ConcurrentHashMap.newKeySet());
        }
        eventsForProcessingGroups.get(commandType).add(policyConfig.processingGroup());
        if (!consumers.containsKey(policyConfig.processingGroup())) {
            consumers.put(policyConfig.processingGroup(), new ConcurrentHashMap<>());
        }
        if (!consumers.get(policyConfig.processingGroup()).containsKey(commandType)) {
            consumers.get(policyConfig.processingGroup()).put(commandType, new ArrayList<>());
        }
        var result = new Registration(aggregateClass, invoker, policyConfig, methodInfo);
        consumers.get(policyConfig.processingGroup()).get(commandType).add(result);
        return result;
    }

    public abstract void start();

    public abstract void stop();

    public abstract void clear();

    protected abstract boolean registerInternal(Class<?> handlerClass);

    public record ProcessingGroupPolicyConfig(String processingGroup, DlqEnqueuePolicy enqueuePolicy, SequencePolicy sequencePolicy) {
    }
    public static ProcessingGroupPolicyConfig defaultProcessingGroupPolicyConfig() {
        return defaultProcessingGroupPolicyConfig("default");
    }

    public static ProcessingGroupPolicyConfig defaultProcessingGroupPolicyConfig(String processingGroup) {
        return new ProcessingGroupPolicyConfig(processingGroup, new DlqEnqueuePolicy() {
            @Override
            public DlqEnqueueDecisionResult shouldEnqueue(InternalMessage message, Throwable error) {
                return DlqEnqueueDecisionResult.ignore();
            }
        }, new NullSequencePolicy());
    }

    protected record HandlerRegistration(ProcessingGroupPolicyConfig policy, Object instance, String methodName) {
    }

    public record Registration(Class<?> handlerClass,
                               TriFunction<Object, Object, Context, Object> method,
                               ProcessingGroupPolicyConfig policyConfig,
                               Method methodInfo) {
    }
}
