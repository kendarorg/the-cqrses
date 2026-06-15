package org.kendar.cqrses.integration.bank;

import org.kendar.cqrses.annotations.AggregateIdentifier;
import org.kendar.cqrses.annotations.Command;

import java.util.UUID;

@Command(version = 1)
public class Withdraw {
    @AggregateIdentifier
    public UUID accountId;
    public long amount;
    public UUID transferId;

    public Withdraw() {
    }

    public Withdraw(UUID accountId, long amount, UUID transferId) {
        this.accountId = accountId;
        this.amount = amount;
        this.transferId = transferId;
    }
}
