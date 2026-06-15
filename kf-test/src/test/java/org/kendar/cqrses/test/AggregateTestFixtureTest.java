package org.kendar.cqrses.test;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.annotations.*;
import org.kendar.cqrses.bus.EventApplyer;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.utils.UUIDGenerator;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class AggregateTestFixtureTest {

    @AfterEach
    void tearDown() {
        GlobalRegistry.clear();
    }

    @Test
    void givenWhenThenHappyPath() {
        UUID id = UUIDGenerator.newUuid();
        new AggregateTestFixture<>(AccountAggregate.class)
                .given(new AccountOpened(id), new Deposited(id, 100))
                .when(new Withdraw(id, 60))
                .expectEvents(new Withdrawn(id, 60))
                .expectResult(40L)
                .expectState(a -> assertEquals(40, a.balance));
    }

    @Test
    void rejectionPathEmitsTheRejectionEvent() {
        UUID id = UUIDGenerator.newUuid();
        new AggregateTestFixture<>(AccountAggregate.class)
                .given(new AccountOpened(id), new Deposited(id, 50))
                .when(new Withdraw(id, 9999))
                .expectEvents(new WithdrawRejected(id, 9999))
                .expectState(a -> assertEquals(50, a.balance));
    }

    @Test
    void expectNoEventsWhenTheHandlerEmitsNothing() {
        UUID id = UUIDGenerator.newUuid();
        new AggregateTestFixture<>(AccountAggregate.class)
                .given(new AccountOpened(id))
                .when(new Deposit(id, -5))
                .expectNoEvents();
    }

    @Test
    void expectExceptionCapturesHandlerThrows() {
        UUID id = UUIDGenerator.newUuid();
        new AggregateTestFixture<>(AccountAggregate.class)
                // no AccountOpened given → handler refuses
                .when(new Withdraw(id, 10))
                .expectException(IllegalStateException.class);
    }

    @Test
    void wrongExpectedEventFailsWithAssertionError() {
        UUID id = UUIDGenerator.newUuid();
        var fixture = new AggregateTestFixture<>(AccountAggregate.class)
                .given(new AccountOpened(id), new Deposited(id, 100))
                .when(new Withdraw(id, 60));
        assertThrows(AssertionError.class,
                () -> fixture.expectEvents(new Withdrawn(id, 61)));
    }

    @Test
    void unexpectedHandlerFailureSurfacesOnExpectations() {
        UUID id = UUIDGenerator.newUuid();
        var fixture = new AggregateTestFixture<>(AccountAggregate.class)
                .when(new Withdraw(id, 10)); // throws: account never opened
        assertThrows(AssertionError.class, fixture::expectNoEvents);
    }

    @Test
    void autoSnapshotTriggersInsideTheFixture() {
        UUID id = UUIDGenerator.newUuid();
        var fixture = new AggregateTestFixture<>(SnapshottingCounter.class)
                .given(new Incremented(id), new Incremented(id))
                .when(new Increment(id))  // 3rd event crosses snapshotEvery=3
                .expectEvents(new Incremented(id));
        var snap = fixture.getEventStore().loadSnapshot(id).orElseThrow();
        assertEquals(2, snap.getAggregateVersion());
        assertEquals(1, snap.getSchemaVersion());
    }

    // ── fixtures-under-test ──────────────────────────────────────────────────

    @Command(version = 1)
    public static class Deposit {
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

    @Command(version = 1)
    public static class Withdraw {
        @AggregateIdentifier
        public UUID accountId;
        public long amount;

        public Withdraw() {
        }

        public Withdraw(UUID accountId, long amount) {
            this.accountId = accountId;
            this.amount = amount;
        }
    }

    @Event(version = 1)
    public static class AccountOpened {
        @AggregateIdentifier
        public UUID accountId;

        public AccountOpened() {
        }

        public AccountOpened(UUID accountId) {
            this.accountId = accountId;
        }
    }

    @Event(version = 1)
    public static class Deposited {
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

    @Event(version = 1)
    public static class Withdrawn {
        @AggregateIdentifier
        public UUID accountId;
        public long amount;

        public Withdrawn() {
        }

        public Withdrawn(UUID accountId, long amount) {
            this.accountId = accountId;
            this.amount = amount;
        }
    }

    @Event(version = 1)
    public static class WithdrawRejected {
        @AggregateIdentifier
        public UUID accountId;
        public long amount;

        public WithdrawRejected() {
        }

        public WithdrawRejected(UUID accountId, long amount) {
            this.accountId = accountId;
            this.amount = amount;
        }
    }

    @Aggregate(group = "accounts")
    public static class AccountAggregate {
        public boolean opened;
        public long balance;

        @CommandHandler
        public void handle(Deposit cmd) {
            if (cmd.amount <= 0) return;
            EventApplyer.apply(this, new Deposited(cmd.accountId, cmd.amount));
        }

        @CommandHandler
        public Long handle(Withdraw cmd) {
            if (!opened) throw new IllegalStateException("account not opened");
            if (balance < cmd.amount) {
                EventApplyer.apply(this, new WithdrawRejected(cmd.accountId, cmd.amount));
                return balance;
            }
            EventApplyer.apply(this, new Withdrawn(cmd.accountId, cmd.amount));
            return balance;
        }

        @EventHandler
        public void on(AccountOpened ignored) {
            opened = true;
        }

        @EventHandler
        public void on(Deposited e) {
            balance += e.amount;
        }

        @EventHandler
        public void on(Withdrawn e) {
            balance -= e.amount;
        }

        @EventHandler
        public void on(WithdrawRejected ignored) {
        }
    }

    @Command(version = 1)
    public static class Increment {
        @AggregateIdentifier
        public UUID id;

        public Increment() {
        }

        public Increment(UUID id) {
            this.id = id;
        }
    }

    @Event(version = 1)
    public static class Incremented {
        @AggregateIdentifier
        public UUID id;

        public Incremented() {
        }

        public Incremented(UUID id) {
            this.id = id;
        }
    }

    public static class CounterSnapshot {
        public int count;

        public CounterSnapshot() {
        }

        public CounterSnapshot(int count) {
            this.count = count;
        }
    }

    @Aggregate(group = "counters", snapshotEvery = 3)
    public static class SnapshottingCounter {
        public int count;

        @CommandHandler
        public void handle(Increment cmd) {
            EventApplyer.apply(this, new Incremented(cmd.id));
        }

        @EventHandler
        public void on(Incremented ignored) {
            count++;
        }

        public CounterSnapshot getSnapshot() {
            return new CounterSnapshot(count);
        }

        public void setSnapshot(CounterSnapshot snap) {
            this.count = snap.count;
        }
    }
}
