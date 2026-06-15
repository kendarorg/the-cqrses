package org.kendar.cqrses.integration.bank;

import org.kendar.cqrses.annotations.Aggregate;
import org.kendar.cqrses.annotations.CommandHandler;
import org.kendar.cqrses.annotations.EventHandler;
import org.kendar.cqrses.bus.EventApplyer;

@Aggregate(group = "transfers-agg")
public class TransferAggregate {
    public boolean requested;

    @CommandHandler
    public void handle(RequestTransfer cmd) {
        if (requested) return;
        EventApplyer.apply(this,
                new TransferRequested(cmd.transferId, cmd.source, cmd.target, cmd.amount));
    }

    @EventHandler
    public void on(TransferRequested ignored) {
        requested = true;
    }
}
