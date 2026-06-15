package org.kendar.pfm.domain;

import org.kendar.cqrses.annotations.Aggregate;
import org.kendar.cqrses.annotations.CommandHandler;
import org.kendar.cqrses.annotations.EventHandler;
import org.kendar.cqrses.bus.EventApplyer;
import org.kendar.pfm.domain.commands.RecordOperation;
import org.kendar.pfm.domain.commands.RegisterUser;
import org.kendar.pfm.domain.events.OperationRecorded;
import org.kendar.pfm.domain.events.UserRegistered;

import java.time.Instant;

/**
 * Event-sourced aggregate, one stream per user (keyed by the deterministic userId). Not a Spring
 * bean: kf instantiates it per-id, so the scanner registers it via {@code GlobalRegistry.register(class)}.
 *
 * <p>The aggregate holds only what it needs to guard command invariants ({@code registered}); the
 * running balance lives in the read model, not here.
 */
@Aggregate(group = "accounts")
public class Account {

    public boolean registered;

    @CommandHandler
    public void handle(RegisterUser cmd) {
        if (registered) return; // idempotent login
        EventApplyer.apply(this, new UserRegistered(cmd.userId, cmd.username));
    }

    @CommandHandler
    public void handle(RecordOperation cmd) {
        if (!registered) return;       // must log in first

        if (cmd.amount <= 0) return;   // reject non-positive amounts
        if (cmd.type == null) return;
        EventApplyer.apply(this, new OperationRecorded(
                cmd.userId, cmd.opId, cmd.type, cmd.amount, cmd.tag, Instant.now().toEpochMilli()));
    }

    @EventHandler
    public void on(UserRegistered ignored) {
        registered = true;
    }

    @EventHandler
    public void on(OperationRecorded ignored) {
        // No aggregate state to fold for the demo: invariants don't depend on past operations.
        // (The handler must exist so EventApplyer can fold the event during the command.)
    }
}
