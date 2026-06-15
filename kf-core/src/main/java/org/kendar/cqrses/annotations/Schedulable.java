package org.kendar.cqrses.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Class-level marker for a bean that carries one or more {@link Schedule}
 * methods. A durable scheduler ({@code JdbcScheduler}) scans
 * {@code GlobalRegistry} instances for this annotation during the setup phase
 * and builds a {@code taskName -> (instance, method, paramType)} dispatch map.
 * Consistent with the {@code @Aggregate}/{@code @Saga}/{@code @Projection}
 * registration idiom.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Schedulable {
}
