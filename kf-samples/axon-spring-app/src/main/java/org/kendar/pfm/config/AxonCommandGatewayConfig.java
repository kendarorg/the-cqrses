package org.kendar.pfm.config;

import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.commandhandling.gateway.DefaultCommandGateway;
import org.axonframework.commandhandling.gateway.IntervalRetryScheduler;
import org.axonframework.commandhandling.gateway.RetryScheduler;
import org.axonframework.modelling.command.ConcurrencyException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Gives the Axon sample the same cross-node optimistic-concurrency tolerance kf has built in, so the
 * two stacks behave identically under the cluster flood.
 *
 * <p><b>Why this is needed.</b> This deployment is server-less: no Axon Server, no distributed command
 * bus, so each JVM has its own local command bus and aggregate repository and there is no
 * single-writer-per-aggregate guarantee across nodes. The flood round-robins each user's
 * {@code RecordOperation}s across all live nodes, so two nodes routinely load the same {@code Account}
 * at sequence N and both try to append event N+1. The {@code UNIQUE(aggregate_identifier,
 * sequence_number)} backstop in {@code domain_event_entry} lets one win; the loser's insert fails and
 * Axon raises a {@link ConcurrencyException} → HTTP 500. The op is then dropped instead of landing.
 *
 * <p>kf hits the identical race (same unique backstop) but {@code ProcessingGroup.invokeCommandSync}
 * retries the command on its {@code OptimisticConcurrencyException} up to 8 times, reloading the
 * aggregate each attempt so the loser re-applies against the winner's just-committed event and lands
 * on the next sequence. This config gives the gateway the Axon-native equivalent.
 *
 * <p>{@link ConcurrencyException} extends {@code AxonTransientException}, and the default predicate
 * ({@code AxonNonTransientExceptionClassesPredicate}) treats only {@code AxonNonTransientException} as
 * non-retryable — so a stock {@link IntervalRetryScheduler} retries exactly this failure. Each retry
 * re-dispatches the command, which reloads the aggregate at the new head, mirroring kf's loop.
 *
 * <p>Axon's starter declares its {@code commandGateway(CommandBus)} bean {@code @ConditionalOnMissingBean}
 * and wires no retry scheduler, so defining the gateway here cleanly replaces it.
 */
@Configuration
public class AxonCommandGatewayConfig {

    /** Mirrors kf's {@code ProcessingGroup.MAX_COMMAND_OCC_RETRIES}. */
    private static final int MAX_RETRY_COUNT = 8;
    /** Backoff per retry, in ms — kf uses a 5ms fixed backoff; a touch higher here since each Axon
     * retry reloads the aggregate from the event store. */
    private static final int RETRY_INTERVAL_MS = 20;

    @Bean
    public RetryScheduler commandRetryScheduler() {
        ScheduledExecutorService retryExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "axon-command-retry");
            t.setDaemon(true);
            return t;
        });
        return IntervalRetryScheduler.builder()
                .retryExecutor(retryExecutor)
                .maxRetryCount(MAX_RETRY_COUNT)
                .retryInterval(RETRY_INTERVAL_MS)
                .build();
    }

    @Bean
    public CommandGateway commandGateway(CommandBus commandBus, RetryScheduler commandRetryScheduler) {
        return DefaultCommandGateway.builder()
                .commandBus(commandBus)
                .retryScheduler(commandRetryScheduler)
                .build();
    }
}
