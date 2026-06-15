package org.kendar.pfm.domain.events;

import org.kendar.cqrses.annotations.AggregateIdentifier;
import org.kendar.cqrses.annotations.Event;
import org.kendar.pfm.domain.OpType;

import java.util.UUID;

@Event(version = 1)
public class OperationRecorded {
    @AggregateIdentifier
    public UUID userId;
    public UUID opId;
    public OpType type;
    public long amount;
    public String tag;
    public long epochMillis;

    public OperationRecorded() {
    }

    public OperationRecorded(UUID userId, UUID opId, OpType type, long amount, String tag, long epochMillis) {
        this.userId = userId;
        this.opId = opId;
        this.type = type;
        this.amount = amount;
        this.tag = tag;
        this.epochMillis = epochMillis;
    }
}
