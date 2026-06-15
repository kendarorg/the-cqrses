package org.kendar.pfm.domain.commands;

import org.axonframework.modelling.command.TargetAggregateIdentifier;
import org.kendar.pfm.domain.OpType;

import java.util.UUID;

/** Record a single IN/OUT operation with one tag against a user's ledger. */
public class RecordOperation {
    @TargetAggregateIdentifier
    public UUID userId;
    public UUID opId;
    public OpType type;
    public long amount;
    public String tag;

    public RecordOperation() {
    }

    public RecordOperation(UUID userId, UUID opId, OpType type, long amount, String tag) {
        this.userId = userId;
        this.opId = opId;
        this.type = type;
        this.amount = amount;
        this.tag = tag;
    }
}
