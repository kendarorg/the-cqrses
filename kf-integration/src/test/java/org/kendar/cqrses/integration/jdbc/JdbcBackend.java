package org.kendar.cqrses.integration.jdbc;

import org.h2.jdbcx.JdbcDataSource;
import org.kendar.cqrses.bus.CommandBus;
import org.kendar.cqrses.bus.EventBus;
import org.kendar.cqrses.bus.JdbcCommandBus;
import org.kendar.cqrses.bus.JdbcEventBus;
import org.kendar.cqrses.db.Db;
import org.kendar.cqrses.db.DefaultDb;
import org.kendar.cqrses.db.SchemaInitializer;
import org.kendar.cqrses.dlq.DlqStore;
import org.kendar.cqrses.integration.IntegrationBackend;
import org.kendar.cqrses.pg.LocalSegmentOwner;
import org.kendar.cqrses.pg.ProcessingGroupsManager;
import org.kendar.cqrses.pg.SegmentCalculator;
import org.kendar.cqrses.pg.SegmentProcessor;
import org.kendar.cqrses.repositories.*;
import org.kendar.cqrses.serialization.MessageSerializer;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link IntegrationBackend} wiring the {@code kf-core-db} JDBC stores and buses
 * over an in-memory H2 database in {@code MODE=MySQL}. {@link #start()} stands up
 * a fresh database and applies the schema; the stores are created lazily on
 * first access (after the suite has registered {@code UpcastersManager} /
 * {@code MessageSerializer}, which {@code JdbcEventStore}'s {@code BaseEventStore}
 * constructor needs) and cached, mirroring the in-memory backend.
 */
public class JdbcBackend implements IntegrationBackend {

    private static final AtomicInteger DB_SEQ = new AtomicInteger();

    private Db db;
    private EventStore eventStore;
    private SagaStore sagaStore;
    private DlqStore dlqStore;
    private CheckpointStore checkpointStore;
    private SegmentProcessor segmentProcessor;
    private LocalSegmentOwner segmentOwner;

    @Override
    public void start() {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:kfbank_" + DB_SEQ.incrementAndGet() + ";MODE=MySQL;DB_CLOSE_DELAY=-1");
        db = new DefaultDb(ds);
        new SchemaInitializer(db).initialize();
    }

    @Override
    public EventStore eventStore() {
        if (eventStore == null) eventStore = new JdbcEventStore(db);
        return eventStore;
    }

    @Override
    public SagaStore sagaStore() {
        if (sagaStore == null) sagaStore = new JdbcSagaStore(db);
        return sagaStore;
    }

    @Override
    public DlqStore dlqStore() {
        if (dlqStore == null) dlqStore = new JdbcDlqStore(db);
        return dlqStore;
    }

    @Override
    public CheckpointStore newCheckpointStore() {
        return new JdbcCheckpointStore(db);
    }

    @Override
    public CommandBus newCommandBus(MessageSerializer serializer) {
        return new JdbcCommandBus(serializer, eventStore(), dlqStore());
    }

    @Override
    public EventBus newEventBus(MessageSerializer serializer) {
        return new JdbcEventBus(serializer, sagaStore(), dlqStore());
    }

    @Override
    public ProcessingGroupsManager handlerOf(EventBus eventBus) {
        return ((JdbcEventBus) eventBus).getHandler();
    }

    @Override
    public boolean eventSidePull() {
        return true;
    }

    @Override
    public void startEventSide(ProcessingGroupsManager eventHandler) {
        checkpointStore = new JdbcCheckpointStore(db);
        segmentProcessor = new SegmentProcessor(eventHandler, eventStore(), checkpointStore);
        segmentOwner = new LocalSegmentOwner(segmentProcessor, SegmentCalculator.getSegments());
        segmentOwner.start();
    }

    @Override
    public void stopEventSide() {
        if (segmentOwner != null) segmentOwner.stop();
        if (segmentProcessor != null) segmentProcessor.stopAll();
    }
}
