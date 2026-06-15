package org.kendar.cqrses.annotations;

import org.kendar.cqrses.saga.JavaBeanPropertyAccessor;
import org.kendar.cqrses.saga.PropertyAccessor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SagaHandler {
    CreationPolicy creationPolicy() default CreationPolicy.NEVER_CREATE;

    String associationProperty() default "";

    String keyName() default "";

    Class<? extends PropertyAccessor> propertyAccessor() default JavaBeanPropertyAccessor.class;
}
