package org.kendar.cqrses.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Method-level marker on a {@link Schedulable} bean carrying the unique logical
 * task name used by {@code Scheduler.schedule(Instant, String, Object)}. The
 * annotated method takes a <strong>single parameter</strong>; on fire its
 * persisted {@code params_json} is deserialised to that declared parameter type
 * and the method is invoked reflectively.
 * <p>
 * More than one {@code @Schedule} method may live on one {@code @Schedulable}
 * class, each with a distinct {@link #value()}.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Schedule {
    /**
     * The unique logical task name. Must be unique across all
     * {@code @Schedulable} beans registered in {@code GlobalRegistry}.
     */
    String value();
}
