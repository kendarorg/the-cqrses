package org.kendar.cqrses.repositories;

import org.kendar.cqrses.annotations.SagaHandler;
import org.kendar.cqrses.annotations.SagaId;
import org.kendar.cqrses.bus.EventBus;
import org.kendar.cqrses.exceptions.InvalidHandlerException;
import org.kendar.cqrses.saga.PropertyAccessor;
import org.kendar.cqrses.utils.ReflectionUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Shared reflection plumbing for {@link SagaStore} implementations: caches the
 * {@code @SagaId} field per saga class and the (keyName, propertyAccessor)
 * pairs derived by scanning {@code @SagaHandler} methods on the saga.
 *
 * <p>Subclasses get two helpers — {@link #extractSagaId(Object)} and
 * {@link #extractCorrelations(Object)} — and decide how to persist/look up
 * the resulting tuples. The interface methods themselves remain abstract so
 * a JDBC implementation can sidestep these caches entirely if it wants to.
 */
public abstract class BaseSagaStore implements SagaStore {

    private static final ConcurrentHashMap<Class<?>, Field> sagaIdFieldCache = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Class<?>, List<CorrelationSpec>> correlationCache = new ConcurrentHashMap<>();

    private static Field findSagaIdField(Class<?> sagaClass) {
        Field cached = sagaIdFieldCache.computeIfAbsent(sagaClass, t -> {
            var fields = ReflectionUtils.getFieldsAnnotatedWith(t, SagaId.class);
            return fields.isEmpty() ? ReflectionUtils.NULL_FIELD : fields.get(0);
        });
        return cached == ReflectionUtils.NULL_FIELD ? null : cached;
    }

    private static List<CorrelationSpec> buildCorrelationSpecs(Class<?> sagaClass) {
        var out = new ArrayList<CorrelationSpec>();
        for (Class<?> c = sagaClass; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Method m : c.getDeclaredMethods()) {
                SagaHandler h = m.getAnnotation(SagaHandler.class);
                if (h == null) continue;
                String property = h.associationProperty();
                if (property == null || property.isEmpty()) continue;
                String key = (h.keyName() == null || h.keyName().isEmpty()) ? property : h.keyName();
                PropertyAccessor accessor = EventBus.getOrCreateAccessor(h.propertyAccessor());
                out.add(new CorrelationSpec(key, property, accessor));
            }
        }
        return out;
    }

    /**
     * Reads the {@code @SagaId} field off {@code saga}, converted to String.
     * Returns {@code null} if the field is missing or its value is {@code null}.
     */
    protected String extractSagaId(Object saga) {
        if (saga == null) return null;
        Field field = findSagaIdField(saga.getClass());
        if (field == null) {
            throw new InvalidHandlerException(
                    "Saga " + saga.getClass().getName() + " has no @SagaId field");
        }
        try {
            Object value = field.get(saga);
            return value == null ? null : value.toString();
        } catch (IllegalAccessException e) {
            throw new RuntimeException(
                    "Cannot read @SagaId field " + field.getName() + " on " + saga.getClass().getName(), e);
        }
    }

    /**
     * Canonical saga {@code type} string — the saga class's simple name.
     */
    protected String extractSagaType(Object saga) {
        return saga.getClass().getSimpleName();
    }

    /**
     * Reads every correlation property declared on the saga's
     * {@code @SagaHandler} methods. Each non-null value becomes a
     * {@code (keyName, value-as-String)} entry. Empty list if the saga has no
     * correlation-bearing handlers.
     */
    protected List<Correlation> extractCorrelations(Object saga) {
        var specs = getCorrelationSpecs(saga.getClass());
        if (specs.isEmpty()) return List.of();
        var out = new ArrayList<Correlation>(specs.size());
        for (CorrelationSpec spec : specs) {
            Object raw = spec.accessor().get(saga, spec.property());
            if (raw == null) continue;
            out.add(new Correlation(spec.key(), raw.toString()));
        }
        return out;
    }

    private List<CorrelationSpec> getCorrelationSpecs(Class<?> sagaClass) {
        return correlationCache.computeIfAbsent(sagaClass, BaseSagaStore::buildCorrelationSpecs);
    }

    /**
     * (keyName, valueAccessor) pair built from a {@code @SagaHandler} on the
     * saga class. {@code key} is the {@code keyName} attribute (defaulting to
     * {@code associationProperty}); {@code property} is the property name read
     * off the saga via {@code accessor}.
     */
    protected record CorrelationSpec(String key, String property, PropertyAccessor accessor) {
    }

    /**
     * A (keyName, value) correlation tuple extracted from a saga instance.
     */
    protected record Correlation(String key, String value) {
    }
}
