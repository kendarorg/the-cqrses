package org.kendar.cqrses.bus;

import org.kendar.cqrses.dlq.DlqStore;
import org.kendar.cqrses.pg.JdbcProcessingGroupsManager;
import org.kendar.cqrses.pg.ProcessingGroupsManager;
import org.kendar.cqrses.repositories.SagaStore;
import org.kendar.cqrses.serialization.MessageSerializer;

/**
 * Event bus for a single-node DB deployment. Identical dispatch to
 * {@code InMemoryEventBus}, constructed with the JDBC {@code SagaStore} /
 * {@code DlqStore}. See {@link JdbcCommandBus} for the durability rationale.
 */
public class JdbcEventBus extends EventBus {

    private final ProcessingGroupsManager handler;

    public JdbcEventBus(MessageSerializer serializer, SagaStore sagaStore, DlqStore dlqStore) {
        super(serializer, sagaStore);
        handler = new JdbcProcessingGroupsManager(this, serializer, dlqStore);
    }

    /**
     * The processing-groups handler that dispatches this bus's events and owns
     * the DLQ re-dispatch path.
     */
    public ProcessingGroupsManager getHandler() {
        return handler;
    }

    @Override
    void send(Object command, Context context) {
        var pgs = eventsForProcessingGroups.get(command.getClass());
        var internalMessage = new InternalMessage();
        internalMessage.setContext(context);
        internalMessage.setEvent(true);
        internalMessage.setPayload(serializer.serialize(command));
        handler.send(pgs, internalMessage);
    }

    @Override
    public void start() {
        handler.start(consumers);
    }

    @Override
    public void stop() {
        handler.stop();
    }

    @Override
    public void clear() {
        handler.clear();
    }
}
