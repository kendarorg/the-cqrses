package org.kendar.cqrses.spring.bank;

import org.kendar.cqrses.annotations.AggregateIdentifier;
import org.kendar.cqrses.annotations.Event;

import java.util.UUID;

@Event(version = 1)
public class AccountOpened {
    @AggregateIdentifier
    public UUID accountId;

    public AccountOpened() {
    }

    public AccountOpened(UUID accountId) {
        this.accountId = accountId;
    }
}
