package org.kendar.cqrses.spring.bank;

import org.kendar.cqrses.annotations.AggregateIdentifier;
import org.kendar.cqrses.annotations.Event;

import java.util.UUID;

@Event(version = 1)
public class Deposited {
    @AggregateIdentifier
    public UUID accountId;
    public long amount;

    public Deposited() {
    }

    public Deposited(UUID accountId, long amount) {
        this.accountId = accountId;
        this.amount = amount;
    }
}
