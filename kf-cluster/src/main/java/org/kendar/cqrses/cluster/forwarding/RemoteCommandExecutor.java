package org.kendar.cqrses.cluster.forwarding;

/**
 * The forwarding server's seam to the local command pipeline: how a command
 * received from a peer is actually executed on this node. Production wires
 * {@link BusRemoteCommandExecutor}; unit tests substitute a stub (template-method
 * style — no mocking framework, per the repo testing strategy).
 */
public interface RemoteCommandExecutor {

    /**
     * Wait mode: run the command synchronously through the local pipeline and
     * return the handler's result ({@code null} for {@code void} handlers).
     * Exceptions propagate — the server turns them into an error response.
     */
    Object executeSync(String typeName, long aggregateVersion, byte[] payload);

    /**
     * Ack mode: enqueue the command into the local async pipeline. Returning
     * normally IS the receipt confirmation; later handler failures follow the
     * owner's DLQ policy exactly as a locally-sent async command would.
     */
    void executeAck(String typeName, long aggregateVersion, byte[] payload);
}
