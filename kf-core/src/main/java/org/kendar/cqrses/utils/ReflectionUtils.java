package org.kendar.cqrses.utils;

import org.kendar.cqrses.annotations.*;
import org.kendar.cqrses.exceptions.InvalidHandlerException;
import org.kendar.cqrses.saga.PropertyAccessor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

public class ReflectionUtils {

    public static final Field NULL_FIELD;

    static {
        try {
            NULL_FIELD = NullFieldHolder.class
                    .getDeclaredField("NULL_FIELD");
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isCommandSide(Class<?> type) {
        if (hasClassAnnotation(type, Aggregate.class)) return true;
        if (hasClassAnnotation(type, CommandInterceptor.class)) return true;
        return hasMethodAnnotation(type, CommandHandler.class);
    }

    public static boolean isEventSide(Class<?> type) {
        if (hasClassAnnotation(type, Saga.class)) return true;
        if (hasClassAnnotation(type, Projection.class)) return true;
        return hasMethodAnnotation(type, EventHandler.class);
    }

    public static boolean hasClassAnnotation(Class<?> type,
                                             Class<? extends Annotation> ann) {
        return type.isAnnotationPresent(ann);
    }



    public static String stackTraceOf(Throwable t) {
        if(t==null) return "";
        var sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }
    public static boolean hasMethodAnnotation(Class<?> type, Class<? extends Annotation> ann) {
        return !getMethodsAnnotatedWith(type, ann).isEmpty();
    }

    /**
     * All methods declared on {@code type} or any superclass (excluding Object)
     * carrying {@code annotation}. Includes non-public methods. Subclass overrides
     * shadow parent methods (deduped by name + parameter types). Each returned
     * Method has setAccessible(true) applied.
     */
    public static List<Method> getMethodsAnnotatedWith(Class<?> type,
                                                       Class<? extends Annotation> annotation) {
        var out = new ArrayList<Method>();
        var seen = new HashSet<MethodKey>();
        for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Method m : c.getDeclaredMethods()) {
                var key = new MethodKey(m.getName(), m.getParameterTypes());
                if (!seen.add(key)) continue;
                if (!m.isAnnotationPresent(annotation)) continue;
                m.setAccessible(true);
                out.add(m);
            }
        }
        return out;
    }

    /**
     * All fields declared on {@code type} or any superclass (excluding Object)
     * carrying {@code annotation}, in class-first then up-the-hierarchy order.
     * A subclass field shadows a parent field of the same name. Includes
     * non-public fields. Each returned Field has setAccessible(true) applied.
     */
    public static List<Field> getFieldsAnnotatedWith(Class<?> type,
                                                     Class<? extends Annotation> annotation) {
        var out = new ArrayList<Field>();
        var seen = new HashSet<String>();
        for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                if (!seen.add(f.getName())) continue;
                if (!f.isAnnotationPresent(annotation)) continue;
                f.setAccessible(true);
                out.add(f);
            }
        }
        return out;
    }

    /**
     * Every type {@code type} can be assigned to: the class itself, all of its
     * superclasses (excluding {@link Object}), and all interfaces it implements
     * transitively (including super-interfaces). Iteration order is the class
     * itself first, then breadth-first up the hierarchy.
     * <p>
     * Used by {@link org.kendar.cqrses.di.GlobalRegistry#register(Class, Object)}
     * to make a single instance resolvable under every type in its hierarchy,
     * not just the key it was registered under.
     */
    public static Set<Class<?>> getAllSupertypes(Class<?> type) {
        var out = new LinkedHashSet<Class<?>>();
        collectSupertypes(type, out);
        return out;
    }

    private static void collectSupertypes(Class<?> type, Set<Class<?>> out) {
        if (type == null || type == Object.class) return;
        if (!out.add(type)) return; // already visited — short-circuit diamond interfaces
        for (Class<?> iface : type.getInterfaces()) {
            collectSupertypes(iface, out);
        }
        collectSupertypes(type.getSuperclass(), out);
    }

    public static PropertyAccessor instantiateAccessor(Class<? extends PropertyAccessor> type) {
        try {
            var ctor = type.getDeclaredConstructor();
            ctor.setAccessible(true);
            return ctor.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new InvalidHandlerException("Cannot instantiate PropertyAccessor " + type.getName());
        }
    }

    private record MethodKey(String name, Class<?>[] params) {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MethodKey mk)) return false;
            return name.equals(mk.name) && Arrays.equals(params, mk.params);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, Arrays.hashCode(params));
        }
    }

    private static class NullFieldHolder {
        @SuppressWarnings("unused")
        public String NULL_FIELD;
    }
}
