package org.kendar.cqrses.serialization;

/**
 * Marker for a class that carries one or more upcasting hops.
 *
 * <p>Implementations don't override any method; they expose
 * {@link org.kendar.cqrses.annotations.UpcasterSpec}-annotated methods (one per version
 * hop, in the style of {@code @EventHandler} methods) of the shape
 * {@code JsonNode upcast(JsonNode node)}. {@link UpcastersManager} discovers those methods
 * at setup time and chains them on the event-store read path.
 */
public interface Upcaster {
}
