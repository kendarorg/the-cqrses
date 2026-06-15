package org.kendar.cqrses.cluster.forwarding;

import org.kendar.cqrses.bus.CommandBus;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.exceptions.InvalidHandlerException;
import org.kendar.cqrses.serialization.MessageSerializer;

/**
 * Production {@link RemoteCommandExecutor}: re-enters the local command pipeline
 * through {@code sendSyncLocal} / {@code sendLocal} — the forward-hook bypasses,
 * so a stale routing table on this side can never ping-pong the command back.
 * The type name is resolved through {@code Bus.getMessageClass} (the framework's
 * canonical simple-name identity; command classes are registered on every node
 * under the frozen per-node topology).
 */
public class BusRemoteCommandExecutor implements RemoteCommandExecutor {

    @Override
    public Object executeSync(String typeName, long aggregateVersion, byte[] payload) {
        var bus = bus();
        return bus.sendSyncLocal(deserialize(bus, typeName, payload), (int) aggregateVersion);
    }

    @Override
    public void executeAck(String typeName, long aggregateVersion, byte[] payload) {
        var bus = bus();
        // Returning from the lane enqueue IS the receipt confirmation; later
        // handler failures follow this node's DLQ policy, exactly as a
        // locally-sent async command would.
        bus.sendLocal(deserialize(bus, typeName, payload), (int) aggregateVersion);
    }

    private CommandBus bus() {
        return GlobalRegistry.get(CommandBus.class);
    }

    private Object deserialize(CommandBus bus, String typeName, byte[] payload) {
        Class<?> commandClass = bus.getMessageClass(typeName);
        if (commandClass == null) {
            throw new InvalidHandlerException(
                    "forwarded command type '" + typeName + "' is not registered on this node");
        }
        return GlobalRegistry.get(MessageSerializer.class).deserialize(payload, commandClass);
    }
}
