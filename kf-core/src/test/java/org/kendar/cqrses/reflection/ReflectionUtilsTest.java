package org.kendar.cqrses.reflection;

import org.junit.jupiter.api.Test;
import org.kendar.cqrses.annotations.*;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.kendar.cqrses.utils.ReflectionUtils.*;

class ReflectionUtilsTest {

    @Test
    void isCommandSideTrueForAggregate() {
        assertTrue(isCommandSide(AggregateFixture.class));
    }

    @Test
    void isCommandSideTrueForInterceptor() {
        assertTrue(isCommandSide(InterceptorFixture.class));
    }

    @Test
    void isCommandSideTrueForMethodCommandHandler() {
        assertTrue(isCommandSide(PlainCommandHandlerFixture.class));
    }

    @Test
    void isCommandSideFalseForUnrelated() {
        assertFalse(isCommandSide(Unannotated.class));
        assertFalse(isCommandSide(SagaFixture.class));
    }

    @Test
    void isEventSideTrueForSagaProjectionAndEventHandler() {
        assertTrue(isEventSide(SagaFixture.class));
        assertTrue(isEventSide(ProjectionFixture.class));
        assertTrue(isEventSide(PlainEventHandlerFixture.class));
    }

    @Test
    void isEventSideFalseForUnrelated() {
        assertFalse(isEventSide(Unannotated.class));
        assertFalse(isEventSide(AggregateFixture.class));
    }

    @Test
    void hasClassAnnotationDetectsPresentAnnotation() {
        assertTrue(hasClassAnnotation(AggregateFixture.class, Aggregate.class));
        assertFalse(hasClassAnnotation(AggregateFixture.class, Saga.class));
    }

    @Test
    void getMethodsAnnotatedWithFindsAnnotatedMethod() {
        List<Method> ms = getMethodsAnnotatedWith(PlainCommandHandlerFixture.class, CommandHandler.class);
        assertEquals(1, ms.size());
        assertEquals("on", ms.get(0).getName());
    }

    @Test
    void getMethodsAnnotatedWithReturnsEmptyWhenAbsent() {
        assertTrue(getMethodsAnnotatedWith(Unannotated.class, CommandHandler.class).isEmpty());
    }

    @Test
    void getMethodsAnnotatedWithSetsMethodsAccessible() {
        List<Method> ms = getMethodsAnnotatedWith(PlainCommandHandlerFixture.class, CommandHandler.class);
        assertTrue(ms.get(0).canAccess(new PlainCommandHandlerFixture()));
    }

    @Test
    void getMethodsAnnotatedWithWalksSuperclass() {
        List<Method> ms = getMethodsAnnotatedWith(Child.class, EventHandler.class);
        assertEquals(1, ms.size());
        assertEquals("parentOnly", ms.get(0).getName());
    }

    @Test
    void getMethodsAnnotatedWithSubclassOverrideShadowsParent() {
        // Child overrides shared() without re-applying @CommandHandler — the
        // override should shadow the parent so the annotation no longer
        // surfaces.
        List<Method> ms = getMethodsAnnotatedWith(Child.class, CommandHandler.class);
        assertTrue(ms.isEmpty(),
                "expected override to shadow the parent's @CommandHandler annotation");
    }

    @Test
    void hasMethodAnnotationMirrorsGetMethodsAnnotatedWith() {
        assertTrue(hasMethodAnnotation(PlainCommandHandlerFixture.class, CommandHandler.class));
        assertFalse(hasMethodAnnotation(Unannotated.class, CommandHandler.class));
    }

    @Test
    void nullFieldSentinelIsNotNull() {
        assertNotNull(NULL_FIELD);
    }

    @Aggregate
    static class AggregateFixture {
        @CommandHandler
        public void onSomething(Object cmd) {
        }
    }

    @CommandInterceptor
    static class InterceptorFixture {
    }

    @Saga
    static class SagaFixture {
    }

    @Projection
    static class ProjectionFixture {
    }

    static class PlainEventHandlerFixture {
        @EventHandler
        public void on(Object event) {
        }
    }

    static class PlainCommandHandlerFixture {
        @CommandHandler
        public void on(Object cmd) {
        }
    }

    static class Unannotated {
        public void noAnnotations() {
        }
    }

    static class Parent {
        @CommandHandler
        public void shared(String s) {
        }

        @EventHandler
        public void parentOnly(Integer i) {
        }
    }

    static class Child extends Parent {
        @Override
        public void shared(String s) {
        }
    }
}
