package org.kendar.cqrses.integration.bank;

import org.kendar.cqrses.annotations.AggregateIdentifier;
import org.kendar.cqrses.annotations.Event;

import java.util.UUID;

@Event(version = 1)
public class TransferRequested {
    @AggregateIdentifier
    public UUID transferId;
    public UUID source;
    public UUID target;
    public long amount;

    public TransferRequested() {
    }

    public TransferRequested(UUID transferId, UUID source, UUID target, long amount) {
        this.transferId = transferId;
        this.source = source;
        this.target = target;
        this.amount = amount;
    }
}
