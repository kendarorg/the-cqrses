package org.kendar.cqrses.spring;

import org.kendar.cqrses.bus.EventBus;

/**
 * Seam for apps that need non-default processing-group policies (or any other {@link EventBus}
 * tweak) beyond the plain {@code kf.processing-groups} list. Every bean of this type is applied to
 * the auto-configured {@link EventBus} at construction time, before the framework freezes its
 * topology.
 */
@FunctionalInterface
public interface EventBusCustomizer {
    void customize(EventBus eventBus);
}
