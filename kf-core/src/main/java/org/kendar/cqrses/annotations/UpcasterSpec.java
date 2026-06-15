package org.kendar.cqrses.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares one upcasting hop on a method of an {@link org.kendar.cqrses.serialization.Upcaster}.
 *
 * <p>The annotated method is the imperative half — it receives the stored payload as a
 * Jackson {@code JsonNode} and returns the rewritten node:
 * <pre>{@code JsonNode upcast(JsonNode node)}</pre>
 * (the parameter/return may be a narrower {@code ObjectNode}). The annotation is the
 * declarative half: it tells {@link org.kendar.cqrses.serialization.UpcastersManager}
 * <em>when</em> to apply the method, replacing the hand-written {@code canUpcast} of
 * classic upcaster designs.
 *
 * <p>Matching is on the message's {@code Context}: {@link #origin()} is compared against
 * {@code Context.type} <strong>case-insensitively, by simple class name only</strong>
 * (which is exactly what the buses store there), and {@link #from()} against
 * {@code Context.version}.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface UpcasterSpec {

    /**
     * Stored event type name — the simple class name as it was written into
     * {@code Context.type}. Kept as a String (never a {@code Class<?>}) on purpose: the
     * origin class may have been renamed or deleted, so it must not be referenced.
     * Matched case-insensitively.
     */
    String origin();

    /** Version this hop upgrades <em>from</em> (matched against {@code Context.version}). */
    int from();

    /**
     * Version this hop produces. {@code 0} (the default) means a single increment —
     * {@code from + 1}. When several hops match the same {@code (origin, from)}, the
     * manager applies the one with the <em>widest</em> effective {@code to} (a direct
     * long jump wins over a step), so a hand-written shortcut can skip intermediate hops.
     */
    int to() default 0;

    /**
     * Target type name to stamp into {@code Context.type} after the transform — used for
     * renames. The default {@code ""} means <strong>no type change</strong>: only the
     * version is bumped.
     */
    String target() default "";
}
