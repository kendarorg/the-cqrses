package org.kendar.cqrses.integration.bank;

import org.kendar.cqrses.annotations.AggregateIdentifier;
import org.kendar.cqrses.annotations.Command;

import java.util.UUID;

@Command(version = 1)
public class OpenAccount {
    @AggregateIdentifier
    public UUID accountId;

    public OpenAccount() {
    }

    public OpenAccount(UUID accountId) {
        this.accountId = accountId;
    }
}
