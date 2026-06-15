package org.kendar.cqrses.integration.memory;

import org.kendar.cqrses.bus.CommandBus;
import org.kendar.cqrses.bus.EventBus;
import org.kendar.cqrses.bus.InMemoryCommandBus;
import org.kendar.cqrses.bus.InMemoryEventBus;
import org.kendar.cqrses.dlq.DlqStore;
import org.kendar.cqrses.integration.IntegrationBackend;
import org.kendar.cqrses.pg.ProcessingGroupsManager;
import org.kendar.cqrses.repositories.*;
import org.kendar.cqrses.serialization.MessageSerializer;

/**
 * {@link IntegrationBackend} wiring the heap stores and buses from
 * {@code kf-core-memory}. No external infrastructure, so {@code start}/{@code stop}
 * stay default no-ops; the stores are created lazily on first access and cached.
 */
public class InMemoryBackend implements IntegrationBackend {

    private EventStore eventStore;
    private SagaStore sagaStore;
    private DlqStore dlqStore;

    @Override
    public EventStore eventStore() {
        if (eventStore == null) eventStore = new InMemoryEventStore();
        return eventStore;
    }

    @Override
    public SagaStore sagaStore() {
        if (sagaStore == null) sagaStore = new InMemorySagaStore();
        return sagaStore;
    }

    @Override
    public DlqStore dlqStore() {
        if (dlqStore == null) dlqStore = new InMemoryDlqStore();
        return dlqStore;
    }

    @Override
    public CheckpointStore newCheckpointStore() {
        return new InMemoryCheckpointStore();
    }

    @Override
    public CommandBus newCommandBus(MessageSerializer serializer) {
        return new InMemoryCommandBus(serializer, eventStore(), dlqStore());
    }

    @Override
    public EventBus newEventBus(MessageSerializer serializer) {
        return new InMemoryEventBus(serializer, sagaStore(), dlqStore());
    }

    @Override
    public ProcessingGroupsManager handlerOf(EventBus eventBus) {
        return ((InMemoryEventBus) eventBus).getHandler();
    }
}
