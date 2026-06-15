package org.kendar.cqrses.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks the field on a {@code @Saga}-annotated class that holds the saga's
 * primary identifier. The field's value is always converted to {@code String}
 * via {@link Object#toString()} when persisted into the saga store.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SagaId {
}
