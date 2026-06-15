package org.kendar.cqrses.annotations;



import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Saga {
    String group() default "default_saga";

    boolean deleteAfterCompletion() default true;
}
