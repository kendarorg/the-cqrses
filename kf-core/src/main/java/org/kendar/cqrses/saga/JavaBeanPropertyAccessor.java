package org.kendar.cqrses.saga;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

public class JavaBeanPropertyAccessor implements PropertyAccessor {

    private static final ConcurrentHashMap<CacheKey, Accessor> CACHE = new ConcurrentHashMap<>();

    private static Accessor resolve(Class<?> type, String property) {
        var getter = "get" + Character.toUpperCase(property.charAt(0)) + property.substring(1);
        for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Method m : c.getDeclaredMethods()) {
                if (m.getParameterCount() == 0 && m.getName().equals(getter)) {
                    m.setAccessible(true);
                    return new Accessor.MethodAccessor(m);
                }
            }
        }
        for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
            try {
                var f = c.getDeclaredField(property);
                f.setAccessible(true);
                return new Accessor.FieldAccessor(f);
            } catch (NoSuchFieldException ignored) {
            }
        }
        return Accessor.MISSING;
    }

    @Override
    public Object get(Object target, String property) {
        if (target == null) return null;
        var accessor = CACHE.computeIfAbsent(
                new CacheKey(target.getClass(), property),
                k -> resolve(k.type(), k.property()));
        try {
            return accessor.read(target);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(
                    "Failed to read property '" + property + "' from " + target.getClass().getName(), e);
        }
    }

    private sealed interface Accessor {
        Accessor MISSING = new Missing();

        Object read(Object target) throws ReflectiveOperationException;

        record MethodAccessor(Method method) implements Accessor {
            public Object read(Object target) throws ReflectiveOperationException {
                return method.invoke(target);
            }
        }

        record FieldAccessor(Field field) implements Accessor {
            public Object read(Object target) throws ReflectiveOperationException {
                return field.get(target);
            }
        }

        record Missing() implements Accessor {
            public Object read(Object target) {
                return null;
            }
        }
    }

    private record CacheKey(Class<?> type, String property) {
    }
}
