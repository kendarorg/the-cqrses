package org.kendar.cqrses.integration.bank;

import org.kendar.cqrses.annotations.Aggregate;
import org.kendar.cqrses.annotations.CommandHandler;
import org.kendar.cqrses.annotations.EventHandler;
import org.kendar.cqrses.bus.EventApplyer;

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
        EventApplyer.apply(this, new Deposited(cmd.accountId, cmd.amount, cmd.transferId));
    }

    @CommandHandler
    public void handle(Withdraw cmd) {
        if (cmd.amount <= 0) return;
        if (balance < cmd.amount) {
            EventApplyer.apply(this, new WithdrawRejected(cmd.accountId, cmd.amount, cmd.transferId));
            return;
        }
        EventApplyer.apply(this, new Withdrawn(cmd.accountId, cmd.amount, cmd.transferId));
    }

    @EventHandler
    public void on(AccountOpened ignored) {
        opened = true;
    }

    @EventHandler
    public void on(Deposited e) {
        balance += e.amount;
    }

    @EventHandler
    public void on(Withdrawn e) {
        balance -= e.amount;
    }

    @EventHandler
    public void on(WithdrawRejected ignored) { /* no-op for balance */ }
}
