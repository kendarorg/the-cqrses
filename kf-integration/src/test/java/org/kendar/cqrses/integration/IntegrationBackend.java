package org.kendar.cqrses.integration;

import org.kendar.cqrses.bus.CommandBus;
import org.kendar.cqrses.bus.EventBus;
import org.kendar.cqrses.dlq.DlqStore;
import org.kendar.cqrses.pg.ProcessingGroupsManager;
import org.kendar.cqrses.repositories.CheckpointStore;
import org.kendar.cqrses.repositories.EventStore;
import org.kendar.cqrses.repositories.SagaStore;
import org.kendar.cqrses.serialization.MessageSerializer;

/**
 * The implementation-specific seam the bank integration suites dispatch through.
 *
 * <p>The suites ({@link AbstractBankLedgerTest}, {@link AbstractBankDlqTest})
 * own all the scenario logic and assertions; everything that differs between a
 * store/bus implementation lives behind this interface: which {@link EventStore}/
 * {@link SagaStore}/{@link DlqStore} backs the run, which concrete bus types are
 * constructed, and how to reach a bus's {@link ProcessingGroupsManager}.
 *
 * <p>Lifecycle: {@link #start()} is called once in {@code @BeforeEach} before any
 * store/bus accessor — that is where a JDBC backend would stand up its in-memory
 * H2 schema. {@link #stop()} is called in {@code @AfterEach} to tear that down.
 * The in-memory backend needs neither, so both default to no-ops.
 *
 * <p>The three store accessors must return the <em>same</em> instances across
 * calls within one run — the buses are wired to them and tests inspect them
 * directly — so an implementation creates them in {@link #start()} and hands the
 * cached references back.
 */
public interface IntegrationBackend {

    /** Stand up any external infrastructure and construct the stores. Called once per test. */
    default void start() {
    }

    /** Tear down whatever {@link #start()} created. Called once per test. */
    default void stop() {
    }

    EventStore eventStore();

    SagaStore sagaStore();

    DlqStore dlqStore();

    /**
     * A {@link CheckpointStore} backed by this implementation (durable for JDBC,
     * heap for in-memory). Used by the cluster pull-pump suites to drive a
     * {@code SegmentProcessor} over this backend's event store.
     */
    CheckpointStore newCheckpointStore();

    /** Construct the implementation's command bus over this backend's stores. */
    CommandBus newCommandBus(MessageSerializer serializer);

    /** Construct the implementation's event bus over this backend's stores. */
    EventBus newEventBus(MessageSerializer serializer);

    /**
     * The live {@link ProcessingGroupsManager} of an event bus produced by
     * {@link #newEventBus}. Used by the DLQ suite to wire a {@code LocalDlqManager}
     * onto the live worker. {@code getHandler()} is not on the {@code EventBus}
     * base type, so the backend bridges to its concrete bus.
     */
    ProcessingGroupsManager handlerOf(EventBus eventBus);

    /**
     * Whether this backend dispatches the event side via the cluster-style
     * <em>pull</em> pump rather than the bus's in-process <em>push</em> lanes.
     *
     * <p>Push backends (in-memory) leave this {@code false}: {@code eventBus.start()}
     * spins up the per-lane worker threads itself. Pull backends (JDBC) return
     * {@code true}: the bus's {@link ProcessingGroupsManager} is put into pull mode
     * before {@code start()}, and a {@code SegmentProcessor} drives dispatch from
     * the event store via {@link #startEventSide}.
     */
    default boolean eventSidePull() {
        return false;
    }

    /**
     * Stand up the pull pump for this backend. Called <em>after</em>
     * {@code eventBus.start()}, with the bus's {@link ProcessingGroupsManager}
     * already switched into pull mode. Push backends never see this called and so
     * default to a no-op.
     */
    default void startEventSide(ProcessingGroupsManager eventHandler) {
    }

    /**
     * Tear the pull pump down. Called first in teardown, before the buses stop.
     * Push backends default to a no-op.
     */
    default void stopEventSide() {
    }
}
