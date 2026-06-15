package org.kendar.cqrses.bus;

import org.kendar.cqrses.annotations.*;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.di.TargetType;
import org.kendar.cqrses.exceptions.InvalidHandlerException;
import org.kendar.cqrses.exceptions.InvalidRegistrationException;
import org.kendar.cqrses.pg.ProcessingGroupsManager;
import org.kendar.cqrses.repositories.SagaStore;
import org.kendar.cqrses.saga.AssociationValue;
import org.kendar.cqrses.saga.PropertyAccessor;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.utils.ReflectionUtils;
import org.kendar.cqrses.utils.UUIDGenerator;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;

import static org.kendar.cqrses.utils.ReflectionUtils.hasClassAnnotation;
import static org.kendar.cqrses.utils.ReflectionUtils.isEventSide;

public abstract class EventBus extends Bus {
    private static final ConcurrentHashMap<Method, SagaAssociationSpec> ASSOCIATION_CACHE = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Class<? extends PropertyAccessor>, PropertyAccessor> ACCESSOR_CACHE = new ConcurrentHashMap<>();
    protected final SagaStore sagaStore;

    public EventBus(MessageSerializer serializer, SagaStore sagaStore) {
        super(serializer);
        this.sagaStore = sagaStore;
    }

    private static Context buildContext(Object event, int aggregateVersion) {
        var commandAnnotation = event.getClass().getAnnotation(Event.class);
        var context = new Context();
        // Identity is the case-folded SIMPLE class name — intentional, to ease
        // upcasting (grill item 3); uniqueness is enforced at registration (item 4).
        context.setType(event.getClass().getSimpleName());
        context.setVersion(commandAnnotation.version());
        context.setTraceId(UUIDGenerator.newUuid());
        context.setAggregateVersion(aggregateVersion);
        var aggregateId = extractAggregateId(event);
        context.setAggregateId(aggregateId);
        return context;
    }

    public static PropertyAccessor getOrCreateAccessor(Class<? extends PropertyAccessor> type) {
        return ACCESSOR_CACHE.computeIfAbsent(type, ReflectionUtils::instantiateAccessor);
    }

    private static SagaAssociationSpec buildSpec(Method method) {
        var sagaHandler = method.getAnnotation(SagaHandler.class);
        if (sagaHandler == null) return null;
        var property = sagaHandler.associationProperty();
        if (property == null || property.isEmpty()) return null;
        var key = sagaHandler.keyName();
        if (key == null || key.isEmpty()) key = property;
        var accessor = getOrCreateAccessor(sagaHandler.propertyAccessor());
        return new SagaAssociationSpec(property, key, accessor);
    }

    public void send(Object command) {

        send(command, -1);
    }

    public void send(Object command, int aggregateVersion) {
        var context = buildContext(command, aggregateVersion);
        send(command, context);

    }

    abstract void send(Object command, Context context);

    /**
     * The event-side {@link ProcessingGroupsManager} this bus dispatches through.
     * Concrete buses override to return their handler; the base returns {@code null}
     * (e.g. test doubles that never build lanes). The cluster pull pump
     * ({@code SegmentProcessor}) needs this to drive lanes/resolvers directly.
     */
    public ProcessingGroupsManager getHandler() {
        return null;
    }

    @Override
    public Object findTarget(Object event, Registration registration) {
        var typeOfTarget = GlobalRegistry.getTargetType(registration.handlerClass());
        if (typeOfTarget == TargetType.PROJECTION) {
            return GlobalRegistry.get(registration.handlerClass());
        } else if (typeOfTarget == TargetType.SAGA) {
            return loadSagaInstance(event, registration);
        } else {
            throw new InvalidHandlerException("Accepted only projections and sagas");
        }
    }

    @Override
    protected boolean registerInternal(Class<?> handlerClass) {
        if (!isEventSide(handlerClass)) return false;
        if (hasClassAnnotation(handlerClass, Projection.class)) {
            subscribeProjection(handlerClass);
        } else if (hasClassAnnotation(handlerClass, Saga.class)) {
            subscribeSaga(handlerClass);
        } else {
            return false;
        }
        return true;
    }

    private void subscribeSaga(Class<?> handlerClass) {
        var methodAnnotation = SagaHandler.class;
        var firstParamAnnotation = Event.class;
        var interceptorAnnotation = handlerClass.getAnnotation(Saga.class);
        var group = interceptorAnnotation.group();
        var policyConfig = resolvePolicyConfig(group);
        // A @SagaId, when declared, must be a UUID: saga ids cross the saga store
        // and (with command forwarding) the wire, so the type is pinned down at
        // registration instead of failing as an opaque toString() later. Absence
        // stays legal here — it still fails at store time (extractSagaId) so
        // store-less fixture sagas keep registering.
        var sagaIdFields = ReflectionUtils.getFieldsAnnotatedWith(handlerClass, SagaId.class);
        if (!sagaIdFields.isEmpty() && sagaIdFields.get(0).getType() != java.util.UUID.class) {
            var field = sagaIdFields.get(0);
            throw new InvalidRegistrationException(
                    "Saga " + handlerClass.getName() + " declares @SagaId "
                            + field.getType().getName() + " " + field.getName()
                            + " — the @SagaId field must be of type java.util.UUID");
        }
        analyzeMethods(handlerClass, methodAnnotation, firstParamAnnotation, policyConfig);
    }

    private void subscribeProjection(Class<?> handlerClass) {
        var methodAnnotation = EventHandler.class;
        var firstParamAnnotation = Event.class;
        var interceptorAnnotation = handlerClass.getAnnotation(Projection.class);
        var group = interceptorAnnotation.group();
        var policyConfig = resolvePolicyConfig(group);
        analyzeMethods(handlerClass, methodAnnotation, firstParamAnnotation, policyConfig);
    }

    protected AssociationValue extractAssociationValue(Object event, Bus.Registration consumer) {
        var spec = ASSOCIATION_CACHE.computeIfAbsent(consumer.methodInfo(), EventBus::buildSpec);
        if (spec == null) return null;
        var raw = spec.accessor().get(event, spec.property());
        if (raw == null) return null;
        var value = new String(raw.toString().getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        return new AssociationValue(spec.key(), value);
    }

    public java.util.Optional<String> resolveSagaId(Object event, Registration reg) {
        var association = extractAssociationValue(event, reg);
        if (association == null) return java.util.Optional.empty();
        var inst = sagaStore.loadSagaByCorrelationId(association.value(), reg.handlerClass().getSimpleName());
        return inst.map(org.kendar.cqrses.repositories.SagaInstance::getId);
    }

    protected Object loadSagaInstance(Object event, Bus.Registration consumer) {
        var association = extractAssociationValue(event, consumer);
        if (association == null) return null;
        var sagaInstance = sagaStore.loadSagaByCorrelationId(association.value(), consumer.handlerClass().getSimpleName());
        if (sagaInstance.isEmpty()) {
            // No existing saga for this correlation. If the receiving method is
            // annotated @SagaStart, this event begins a new saga lifecycle —
            // instantiate a fresh instance via the no-arg ctor so the handler
            // can populate @SagaId / association fields, and the bus will
            // persist it after the handler returns. Otherwise the event is
            // simply not for any saga of this type → null (skip).
            if (consumer.methodInfo() == null
                    || consumer.methodInfo().getAnnotation(org.kendar.cqrses.annotations.SagaStart.class) == null) {
                return null;
            }
            try {
                return consumer.handlerClass().getDeclaredConstructor().newInstance();
            } catch (ReflectiveOperationException e) {
                throw new org.kendar.cqrses.exceptions.InvalidHandlerException(
                        "Cannot instantiate saga " + consumer.handlerClass().getName() +
                                " for @SagaStart on " + event.getClass().getName());
            }
        }
        var data = sagaInstance.get().getContent();
        return serializer.deserialize(data, consumer.handlerClass());
    }

    private record SagaAssociationSpec(String property, String key, PropertyAccessor accessor) {
    }
}
