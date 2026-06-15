package org.kendar.cqrses.bus;

import org.kendar.cqrses.dlq.DlqStore;
import org.kendar.cqrses.pg.ProcessingGroupsManager;
import org.kendar.cqrses.repositories.SagaStore;
import org.kendar.cqrses.serialization.MessageSerializer;

public class InMemoryEventBus extends EventBus {

    private final ProcessingGroupsManager handler;

    public InMemoryEventBus(MessageSerializer serializer, SagaStore sagaStore,DlqStore dlqStore) {
        super(serializer, sagaStore);
        handler = new ProcessingGroupsManager(this, serializer, dlqStore );
    }

    /**
     * The processing-groups handler that dispatches this bus's events and owns
     * the DLQ re-dispatch path. Wire it into an {@code AxonStyleDlqManager} to
     * retry dead letters routed by this bus's {@code DLQ_*} policies.
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
