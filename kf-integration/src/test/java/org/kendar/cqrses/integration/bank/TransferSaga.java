package org.kendar.cqrses.integration.bank;

import org.kendar.cqrses.annotations.Saga;
import org.kendar.cqrses.annotations.SagaHandler;
import org.kendar.cqrses.annotations.SagaId;
import org.kendar.cqrses.annotations.SagaStart;
import org.kendar.cqrses.bus.CommandBus;
import org.kendar.cqrses.di.GlobalRegistry;

import java.util.UUID;

@Saga(group = "transfers-saga")
public class TransferSaga {
    @SagaId
    public UUID transferId;
    public UUID source;
    public UUID target;
    public long amount;
    public Status status = Status.PENDING;

    @SagaStart
    @SagaHandler(associationProperty = "transferId")
    public void on(TransferRequested e) {
        this.transferId = e.transferId;
        this.source = e.source;
        this.target = e.target;
        this.amount = e.amount;
        GlobalRegistry.get(CommandBus.class)
                .send(new Withdraw(source, amount, transferId));
    }

    @SagaHandler(associationProperty = "transferId")
    public void on(Withdrawn e) {
        // Source withdraw accepted — release the matching deposit on the target.
        GlobalRegistry.get(CommandBus.class)
                .send(new Deposit(target, e.amount, transferId));
    }

    @SagaHandler(associationProperty = "transferId")
    public void on(Deposited ignored) {
        status = Status.COMPLETED;
    }

    @SagaHandler(associationProperty = "transferId")
    public void on(WithdrawRejected ignored) {
        status = Status.FAILED;
    }

    public enum Status {PENDING, COMPLETED, FAILED}
}
