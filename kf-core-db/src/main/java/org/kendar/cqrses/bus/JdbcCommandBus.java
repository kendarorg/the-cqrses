package org.kendar.cqrses.bus;

import org.kendar.cqrses.dlq.DlqStore;
import org.kendar.cqrses.pg.JdbcProcessingGroupsManager;
import org.kendar.cqrses.pg.ProcessingGroupsManager;
import org.kendar.cqrses.repositories.EventStore;
import org.kendar.cqrses.serialization.MessageSerializer;

/**
 * Command bus for a single-node DB deployment. Identical dispatch to
 * {@code InMemoryCommandBus} — the lane/segment/resolver/policy/DLQ machinery in
 * {@link ProcessingGroupsManager} is reused unchanged — but constructed with the
 * JDBC stores. Durability of the message flow comes from those stores
 * (commands → durable {@code JdbcEventStore}, failures → durable
 * {@code JdbcDlqStore}), not from a durable bus queue; the in-flight lane queue
 * stays in heap and is recoverable by replaying committed {@code event_entry}
 * rows. A durable, pollable per-{@code (group,segment)} work queue is the
 * cluster module's job, deliberately not built here.
 */
public class JdbcCommandBus extends CommandBus {
    private final ProcessingGroupsManager handler;

    public JdbcCommandBus(MessageSerializer serializer, EventStore eventStore, DlqStore dlqStore) {
        super(serializer, eventStore);
        handler = new JdbcProcessingGroupsManager(this, serializer, dlqStore);
    }

    /**
     * The processing-groups handler that dispatches this bus's commands and owns
     * the DLQ re-dispatch path.
     */
    public ProcessingGroupsManager getHandler() {
        return handler;
    }

    @Override
    Object sendSync(Object command, Context context) {
        var pgs = eventsForProcessingGroups.get(command.getClass());
        var internalMessage = new InternalMessage();
        internalMessage.setContext(context);
        internalMessage.setEvent(false);
        internalMessage.setPayload(serializer.serialize(command));
        return handler.sendSync(pgs, consumers, internalMessage);
    }

    @Override
    void send(Object command, Context context) {
        var pgs = eventsForProcessingGroups.get(command.getClass());
        var internalMessage = new InternalMessage();
        internalMessage.setContext(context);
        internalMessage.setEvent(false);
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
