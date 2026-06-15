package org.kendar.cqrses.spring.bank;

import org.kendar.cqrses.annotations.AggregateIdentifier;
import org.kendar.cqrses.annotations.Command;

import java.util.UUID;

@Command(version = 1)
public class Deposit {
    @AggregateIdentifier
    public UUID accountId;
    public long amount;

    public Deposit() {
    }

    public Deposit(UUID accountId, long amount) {
        this.accountId = accountId;
        this.amount = amount;
    }
}
