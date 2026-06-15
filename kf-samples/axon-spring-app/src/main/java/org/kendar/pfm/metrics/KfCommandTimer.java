package org.kendar.pfm.metrics;

import org.axonframework.commandhandling.CommandBus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Command-handler timing hook: registers a {@code MessageHandlerInterceptor} on the {@link CommandBus}
 * that times the interceptor chain around each command and records {@code kf.command.handle}
 * (group="accounts", type=command simpleName) into {@link KfMeters} — the kf-parity command latency.
 *
 * <p>Deliberately a <b>separate</b> bean from {@code KfMetricsConfig}: that class produces a
 * {@code ConfigurerModule} which Axon's {@code springAxonConfigurer} depends on, so if the same bean
 * also injected the {@link CommandBus} (which is built <i>from</i> that configurer) Spring would see a
 * circular reference. Injecting the bus into this standalone component instead keeps the graph acyclic
 * — the bus is fully built before this bean's {@link Autowired} method runs.
 */
@Component
public class KfCommandTimer {

    @Autowired
    public void register(CommandBus commandBus, KfMeters meters) {
        commandBus.registerHandlerInterceptor((unitOfWork, chain) -> {
            long t0 = System.nanoTime();
            boolean ok = true;
            try {
                return chain.proceed();
            } catch (Exception e) {
                ok = false;
                throw e;
            } finally {
                meters.onCommandHandled("accounts",
                        unitOfWork.getMessage().getPayloadType().getSimpleName(),
                        System.nanoTime() - t0, ok);
            }
        });
    }
}
