package org.kendar.cqrses.integration.bank;

import org.kendar.cqrses.annotations.AggregateIdentifier;
import org.kendar.cqrses.annotations.Event;

import java.util.UUID;

@Event(version = 1)
public class WithdrawRejected {
    @AggregateIdentifier
    public UUID accountId;
    public long amount;
    public UUID transferId;

    public WithdrawRejected() {
    }

    public WithdrawRejected(UUID accountId, long amount, UUID transferId) {
        this.accountId = accountId;
        this.amount = amount;
        this.transferId = transferId;
    }
}
