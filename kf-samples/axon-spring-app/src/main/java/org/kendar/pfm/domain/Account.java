package org.kendar.pfm.domain;

import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.eventsourcing.EventSourcingHandler;
import org.axonframework.modelling.command.AggregateCreationPolicy;
import org.axonframework.modelling.command.AggregateIdentifier;
import org.axonframework.modelling.command.CreationPolicy;
import org.axonframework.spring.stereotype.Aggregate;
import org.kendar.pfm.domain.commands.RecordOperation;
import org.kendar.pfm.domain.commands.RegisterUser;
import org.kendar.pfm.domain.events.OperationRecorded;
import org.kendar.pfm.domain.events.UserRegistered;

import java.time.Instant;
import java.util.UUID;

import static org.axonframework.modelling.command.AggregateLifecycle.apply;

/**
 * Event-sourced aggregate, one stream per user (keyed by the deterministic userId). Behavioural
 * twin of the kf sample's {@code Account}: the aggregate holds only what it needs to guard command
 * invariants ({@code registered}); the running balance lives in the read model.
 *
 * <p>Axon equivalents of the kf annotations: {@code @Aggregate} ⇄ {@code @Aggregate},
 * {@code @CommandHandler} ⇄ {@code @CommandHandler}, {@code @EventHandler}-on-aggregate ⇄
 * {@code @EventSourcingHandler}, {@code EventApplyer.apply} ⇄ {@code AggregateLifecycle.apply}.
 * {@code RegisterUser} uses {@link AggregateCreationPolicy#CREATE_IF_MISSING} so a repeated login
 * loads the existing aggregate instead of failing with "aggregate already exists" — the idempotent
 * login the kf sample gets for free by ignoring the command when {@code registered}.
 */
@Aggregate
public class Account {

    @AggregateIdentifier
    private UUID userId;
    private boolean registered;

    public Account() {
    }

    @CommandHandler
    @CreationPolicy(AggregateCreationPolicy.CREATE_IF_MISSING)
    public void handle(RegisterUser cmd) {
        if (registered) {
            return; // idempotent login
        }
        apply(new UserRegistered(cmd.userId, cmd.username));
    }

    @CommandHandler
    public void handle(RecordOperation cmd) {
        if (!registered) {
            return; // must log in first
        }
        if (cmd.amount <= 0) {
            return; // reject non-positive amounts
        }
        if (cmd.type == null) {
            return;
        }
        apply(new OperationRecorded(
                cmd.userId, cmd.opId, cmd.type, cmd.amount, cmd.tag, Instant.now().toEpochMilli()));
    }

    @EventSourcingHandler
    public void on(UserRegistered e) {
        this.userId = e.userId; // sets the aggregate id on the creation event
        this.registered = true;
    }

    @EventSourcingHandler
    public void on(OperationRecorded ignored) {
        // No aggregate state to fold for the demo: invariants don't depend on past operations.
    }
}
