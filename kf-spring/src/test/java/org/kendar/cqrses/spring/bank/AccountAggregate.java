package org.kendar.cqrses.spring.bank;

import org.kendar.cqrses.annotations.Aggregate;
import org.kendar.cqrses.annotations.CommandHandler;
import org.kendar.cqrses.annotations.EventHandler;
import org.kendar.cqrses.bus.EventApplyer;

/**
 * Not a Spring bean: kf instantiates it per-id, so the scanner registers it with
 * {@code GlobalRegistry.register(class)}.
 */
@Aggregate(group = "accounts")
public class AccountAggregate {
    public boolean opened;
    public long balance;

    @CommandHandler
    public void handle(OpenAccount cmd) {
        if (opened) return;
        EventApplyer.apply(this, new AccountOpened(cmd.accountId));
    }

    @CommandHandler
    public void handle(Deposit cmd) {
        if (cmd.amount <= 0) return;
        EventApplyer.apply(this, new Deposited(cmd.accountId, cmd.amount));
    }

    @EventHandler
    public void on(AccountOpened ignored) {
        opened = true;
    }

    @EventHandler
    public void on(Deposited e) {
        balance += e.amount;
    }
}
