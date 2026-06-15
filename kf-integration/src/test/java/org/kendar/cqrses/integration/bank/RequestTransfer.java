package org.kendar.cqrses.integration.bank;

import org.kendar.cqrses.annotations.AggregateIdentifier;
import org.kendar.cqrses.annotations.Command;

import java.util.UUID;

@Command(version = 1)
public class RequestTransfer {
    @AggregateIdentifier
    public UUID transferId;
    public UUID source;
    public UUID target;
    public long amount;

    public RequestTransfer() {
    }

    public RequestTransfer(UUID transferId, UUID source, UUID target, long amount) {
        this.transferId = transferId;
        this.source = source;
        this.target = target;
        this.amount = amount;
    }
}
