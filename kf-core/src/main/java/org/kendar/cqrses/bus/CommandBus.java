package org.kendar.cqrses.bus;

import org.kendar.cqrses.annotations.*;
import org.kendar.cqrses.cluster.spi.CommandForwarding;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.di.TargetType;
import org.kendar.cqrses.exceptions.InvalidHandlerException;
import org.kendar.cqrses.exceptions.InvalidRegistrationException;
import org.kendar.cqrses.observability.TraceRecorder;
import org.kendar.cqrses.repositories.EventStore;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.utils.UUIDGenerator;

import static org.kendar.cqrses.utils.ReflectionUtils.hasClassAnnotation;
import static org.kendar.cqrses.utils.ReflectionUtils.isCommandSide;

public abstract class CommandBus extends Bus {
    protected final EventStore eventStore;

    public CommandBus(MessageSerializer serializer, EventStore eventStore) {
        super(serializer);
        this.eventStore = eventStore;
    }

    private static Context buildContext(Object command, int aggregateVersion) {
        var commandAnnotation = command.getClass().getAnnotation(Command.class);
        var context = new Context();
        // Identity is the case-folded SIMPLE class name — intentional, to ease
        // upcasting (grill item 3): the upcaster keeps origin as a String and rewrites
        // the JSON tree, so the origin class can be moved between packages or deleted
        // and replay still works. Uniqueness is enforced at registration (item 4).
        context.setType(command.getClass().getSimpleName());
        context.setVersion(commandAnnotation.version());
        context.setTraceId(UUIDGenerator.newUuid());
        context.setAggregateVersion(aggregateVersion);
        var aggregateId = extractAggregateId(command);
        context.setAggregateId(aggregateId);
        return context;
    }

    /**
     * Synchronous dispatch returning the {@code @CommandHandler}'s return value
     * (Axon's {@code sendAndWait} contract): the whole pipeline runs on the
     * caller's thread, handler exceptions propagate to the sender, and a
     * non-{@code void} handler's result — a generated id, the new state, a
     * rejection reason — comes back to the caller. {@code void} handlers (and
     * commands with no matching handler) yield {@code null}.
     */
    public <R> R sendSync(Object command) {
        return sendSync(command, -1);
    }

    @SuppressWarnings("unchecked")
    public <R> R sendSync(Object command, int aggregateVersion) {
        var context = buildContext(command, aggregateVersion);
        var forwarder = CommandForwarding.current();
        if (forwarder != null) {
            // RemoteCommandException / ForwardTimeoutException propagate to the
            // sender; the local trace span is not opened — the owner records its
            // own full pipeline trace.
            var forwarded = forwarder.forwardSync(command, context);
            if (forwarded.isPresent()) return (R) forwarded.get().value();
        }
        return dispatchSyncLocal(command, context);
    }

    /**
     * Local pipeline entry bypassing the {@code CommandForwarding} hook. Used by
     * the cluster's forwarding server to execute a command received from a peer —
     * re-entering {@code sendSync} there could ping-pong the command between two
     * nodes with stale routing tables.
     */
    public <R> R sendSyncLocal(Object command) {
        return sendSyncLocal(command, -1);
    }

    public <R> R sendSyncLocal(Object command, int aggregateVersion) {
        return dispatchSyncLocal(command, buildContext(command, aggregateVersion));
    }

    @SuppressWarnings("unchecked")
    private <R> R dispatchSyncLocal(Object command, Context context) {
        // The whole synchronous pipeline (rehydrate -> handler -> append -> publish)
        // runs on this thread, so the sampled trace begun here sees every stage.
        boolean sampled = TraceRecorder.begin(context.getTraceId(), context.getType(), context.getAggregateId());
        boolean ok = false;
        try {
            Object result = sendSync(command, context);
            ok = true;
            return (R) result;
        } finally {
            if (sampled) TraceRecorder.end(ok);
        }
    }

    abstract Object sendSync(Object command, Context context);

    public void send(Object command) {
        send(command, -1);
    }

    public void send(Object command, int aggregateVersion) {
        var context = buildContext(command, aggregateVersion);
        var forwarder = CommandForwarding.current();
        if (forwarder != null && forwarder.forwardAsync(command, context)) return;
        send(command, context);
    }

    /** Async counterpart of {@link #sendSyncLocal(Object)} — bypasses the forward hook. */
    public void sendLocal(Object command) {
        sendLocal(command, -1);
    }

    public void sendLocal(Object command, int aggregateVersion) {
        send(command, buildContext(command, aggregateVersion));
    }

    @Override
    public Object findTarget(Object command, Registration registration) {
        var typeOfTarget = GlobalRegistry.getTargetType(registration.handlerClass());
        if (typeOfTarget == TargetType.AGGREGATE) {
            return rehydrateAggregate(command, registration);
        } else if (typeOfTarget == TargetType.COMMAND_HANDLER) {
            return GlobalRegistry.get(registration.handlerClass());
        } else {
            throw new InvalidHandlerException("Accepted only command handlers and aggregates");
        }
    }


    abstract void send(Object command, Context context);

    @Override
    protected boolean registerInternal(Class<?> handlerClass) {
        if (!isCommandSide(handlerClass)) return false;
        if (hasClassAnnotation(handlerClass, Aggregate.class)) {
            subscribeAggregate(handlerClass);
        } else if (hasClassAnnotation(handlerClass, CommandInterceptor.class)) {
            subscribeInterceptor(handlerClass);
        } else {
            return false;
        }
        return true;
    }

    private void subscribeInterceptor(Class<?> handlerClass) {
        var methodAnnotation = CommandInterceptor.class;
        var firstParamAnnotation = Command.class;
        var interceptorAnnotation = handlerClass.getAnnotation(CommandInterceptor.class);
        var group = interceptorAnnotation.group();

        ProcessingGroupPolicyConfig policyConfig = resolvePolicyConfig(group);
        analyzeMethods(handlerClass, methodAnnotation, firstParamAnnotation, policyConfig);
    }


    private void subscribeAggregate(Class<?> aggregateClass) {
        var methodAnnotation = CommandHandler.class;
        var firstParamAnnotation = Command.class;
        var aggregateAnnotation = aggregateClass.getAnnotation(Aggregate.class);
        var group = aggregateAnnotation.group();

        ProcessingGroupPolicyConfig policyConfig = resolvePolicyConfig(group);
        var registrations = analyzeMethods(aggregateClass, methodAnnotation, firstParamAnnotation, policyConfig);
        // Aggregate commands MUST carry a UUID @AggregateIdentifier: without one,
        // extractAggregateId returns null and the failure surfaces only at dispatch
        // time (segment hashing / rehydration). Interceptor-only commands stay
        // exempt — they legitimately may carry no id (routed to segment 0).
        for (var registration : registrations) {
            var commandType = registration.methodInfo().getParameterTypes()[0];
            var idField = GlobalRegistry.getFieldAnnotatedWith(commandType, AggregateIdentifier.class);
            if (idField == null || idField.getType() != java.util.UUID.class) {
                throw new InvalidRegistrationException(
                        "Command " + commandType.getName() + " handled by aggregate " + aggregateClass.getName()
                                + " must declare a java.util.UUID field annotated @AggregateIdentifier"
                                + (idField == null
                                ? " (no annotated field found)"
                                : " (found " + idField.getType().getName() + " " + idField.getName() + ")"));
            }
        }
    }

    protected Object rehydrateAggregate(Object command, Bus.Registration consumer) {
        var aggregateId = extractAggregateId(command);
        var aggregate = eventStore.loadAggregate(aggregateId, consumer.handlerClass());

        var methodInfo = consumer.methodInfo();
        var commandHandler = methodInfo.getAnnotation(CommandHandler.class);
        try {
            if (commandHandler != null) {
                if ((commandHandler.creationPolicy() == CreationPolicy.CREATE_IF_NOT_EXISTS && aggregate.isEmpty())
                        || commandHandler.creationPolicy() == CreationPolicy.ALWAYS_CREATE) {
                    return consumer.handlerClass().getDeclaredConstructor().newInstance();
                }
            }
        } catch (Exception e) {
            throw new InvalidHandlerException("Cannot create instance for " + consumer.handlerClass().getName());
        }
        return aggregate.orElse(null);
    }

}
