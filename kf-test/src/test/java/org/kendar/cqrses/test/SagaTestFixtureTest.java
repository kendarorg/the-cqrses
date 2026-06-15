package org.kendar.cqrses.test;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.annotations.*;
import org.kendar.cqrses.bus.CommandBus;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.utils.UUIDGenerator;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class SagaTestFixtureTest {

    @AfterEach
    void tearDown() {
        GlobalRegistry.clear();
    }

    @Test
    void sagaStartDispatchesTheFirstCommand() {
        UUID transferId = UUIDGenerator.newUuid();
        UUID source = UUIDGenerator.newUuid();
        UUID target = UUIDGenerator.newUuid();
        new SagaTestFixture<>(TransferSaga.class)
                .whenPublishing(new TransferRequested(transferId, source, target, 60))
                .expectDispatchedCommands(new Withdraw(source, 60, transferId))
                .expectSaga(transferId, s -> {
                    assertEquals(TransferSaga.Status.PENDING, s.status);
                    assertEquals(source, s.source);
                });
    }

    @Test
    void midSagaEventDispatchesTheFollowUpCommand() {
        UUID transferId = UUIDGenerator.newUuid();
        UUID source = UUIDGenerator.newUuid();
        UUID target = UUIDGenerator.newUuid();
        new SagaTestFixture<>(TransferSaga.class)
                .given(new TransferRequested(transferId, source, target, 60))
                .whenPublishing(new Withdrawn(source, 60, transferId))
                .expectDispatchedCommands(new Deposit(target, 60, transferId))
                .expectSaga(transferId, s -> assertEquals(TransferSaga.Status.PENDING, s.status));
    }

    @Test
    void terminalEventCompletesTheSagaWithoutCommands() {
        UUID transferId = UUIDGenerator.newUuid();
        UUID source = UUIDGenerator.newUuid();
        UUID target = UUIDGenerator.newUuid();
        new SagaTestFixture<>(TransferSaga.class)
                .given(new TransferRequested(transferId, source, target, 60),
                        new Withdrawn(source, 60, transferId))
                .whenPublishing(new Deposited(target, 60, transferId))
                .expectNoDispatchedCommands()
                .expectSaga(transferId, s -> assertEquals(TransferSaga.Status.COMPLETED, s.status));
    }

    @Test
    void unrelatedCorrelationDoesNotTouchTheSaga() {
        UUID transferId = UUIDGenerator.newUuid();
        UUID other = UUIDGenerator.newUuid();
        UUID source = UUIDGenerator.newUuid();
        new SagaTestFixture<>(TransferSaga.class)
                .given(new TransferRequested(transferId, source, UUIDGenerator.newUuid(), 60))
                .whenPublishing(new Withdrawn(source, 60, other))
                .expectNoDispatchedCommands()
                .expectNoSaga(other);
    }

    @Test
    void throwingSagaHandlerFailsTheTest() {
        UUID transferId = UUIDGenerator.newUuid();
        var fixture = new SagaTestFixture<>(ExplodingSaga.class);
        assertThrows(AssertionError.class,
                () -> fixture.whenPublishing(
                        new TransferRequested(transferId, UUIDGenerator.newUuid(),
                                UUIDGenerator.newUuid(), 1)));
    }

    @Test
    void wrongExpectedCommandFailsWithAssertionError() {
        UUID transferId = UUIDGenerator.newUuid();
        UUID source = UUIDGenerator.newUuid();
        var fixture = new SagaTestFixture<>(TransferSaga.class)
                .whenPublishing(new TransferRequested(transferId, source,
                        UUIDGenerator.newUuid(), 60));
        assertThrows(AssertionError.class,
                () -> fixture.expectDispatchedCommands(new Withdraw(source, 61, transferId)));
    }

    // ── fixtures-under-test ──────────────────────────────────────────────────

    @Command(version = 1)
    public static class Withdraw {
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

    @Command(version = 1)
    public static class Deposit {
        @AggregateIdentifier
        public UUID accountId;
        public long amount;
        public UUID transferId;

        public Deposit() {
        }

        public Deposit(UUID accountId, long amount, UUID transferId) {
            this.accountId = accountId;
            this.amount = amount;
            this.transferId = transferId;
        }
    }

    @Event(version = 1)
    public static class TransferRequested {
        @AggregateIdentifier
        public UUID transferId;
        public UUID source;
        public UUID target;
        public long amount;

        public TransferRequested() {
        }

        public TransferRequested(UUID transferId, UUID source, UUID target, long amount) {
            this.transferId = transferId;
            this.source = source;
            this.target = target;
            this.amount = amount;
        }
    }

    @Event(version = 1)
    public static class Withdrawn {
        @AggregateIdentifier
        public UUID accountId;
        public long amount;
        public UUID transferId;

        public Withdrawn() {
        }

        public Withdrawn(UUID accountId, long amount, UUID transferId) {
            this.accountId = accountId;
            this.amount = amount;
            this.transferId = transferId;
        }
    }

    @Event(version = 1)
    public static class Deposited {
        @AggregateIdentifier
        public UUID accountId;
        public long amount;
        public UUID transferId;

        public Deposited() {
        }

        public Deposited(UUID accountId, long amount, UUID transferId) {
            this.accountId = accountId;
            this.amount = amount;
            this.transferId = transferId;
        }
    }

    @Saga(group = "transfers-saga")
    public static class TransferSaga {
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
            GlobalRegistry.get(CommandBus.class).send(new Withdraw(source, amount, transferId));
        }

        @SagaHandler(associationProperty = "transferId")
        public void on(Withdrawn e) {
            GlobalRegistry.get(CommandBus.class).send(new Deposit(target, e.amount, transferId));
        }

        @SagaHandler(associationProperty = "transferId")
        public void on(Deposited ignored) {
            status = Status.COMPLETED;
        }

        public enum Status {PENDING, COMPLETED}
    }

    @Saga(group = "exploding-saga")
    public static class ExplodingSaga {
        @SagaId
        public UUID transferId;

        @SagaStart
        @SagaHandler(associationProperty = "transferId")
        public void on(TransferRequested e) {
            throw new IllegalStateException("boom");
        }
    }
}
