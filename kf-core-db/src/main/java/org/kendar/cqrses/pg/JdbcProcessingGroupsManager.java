package org.kendar.cqrses.pg;

import org.kendar.cqrses.bus.Bus;
import org.kendar.cqrses.dlq.DlqStore;
import org.kendar.cqrses.serialization.MessageSerializer;

import java.util.List;
import java.util.Map;

public class JdbcProcessingGroupsManager extends ProcessingGroupsManager{
    public JdbcProcessingGroupsManager(Bus bus, MessageSerializer serializer, DlqStore dlqStore) {
        super(bus, serializer, dlqStore);
    }


    protected ProcessingGroup createProcessingGroup(String group, Map<Class<?>, List<Bus.Registration>> consumer, Bus.ProcessingGroupPolicyConfig policy) {
        return new JdbcProcessingGroup(group, bus, serializer, commandSide, dlqStore, consumer, policy);
    }
}
