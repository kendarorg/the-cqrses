package org.kendar.cqrses.integration.bank;

import org.kendar.cqrses.annotations.AggregateIdentifier;
import org.kendar.cqrses.annotations.Command;

import java.util.UUID;

@Command(version = 1)
public class Deposit {
    @AggregateIdentifier
    public UUID accountId;
    public long amount;
    public UUID transferId; // null when the deposit isn't part of a transfer

    public Deposit() {
    }

    public Deposit(UUID accountId, long amount, UUID transferId) {
        this.accountId = accountId;
        this.amount = amount;
        this.transferId = transferId;
    }
}
