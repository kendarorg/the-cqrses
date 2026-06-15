package org.kendar.cqrses.cluster.forwarding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Lazily-created {@link ForwardingClient} per peer node id. A client found dead
 * on lookup is replaced; {@link #invalidate} force-discards after a transport
 * failure so the next route attempt reconnects against fresh routing data.
 * <p>
 * {@link #close(long)} is the shutdown drain: it waits up to the budget for the
 * pending request count to reach zero, then closes everything — leftover
 * futures fail with {@link TransportException}, which the forwarder surfaces
 * per its timeout semantics.
 */
public class ForwardingClientPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(ForwardingClientPool.class.getName());

    private final long connectTimeoutMillis;
    private final ConcurrentHashMap<String, ForwardingClient> clients = new ConcurrentHashMap<>();
    private volatile boolean closed;

    public ForwardingClientPool(long connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    /**
     * The live client for {@code nodeId}, connecting if absent or dead.
     *
     * @throws TransportException when connecting fails or the pool is closed
     */
    public ForwardingClient clientFor(String nodeId, String host, int port) {
        if (closed) {
            throw new TransportException("forwarding client pool is closed");
        }
        return clients.compute(nodeId, (id, existing) -> {
            if (existing != null && existing.isAlive()) return existing;
            if (existing != null) existing.close();
            return new ForwardingClient(host, port, connectTimeoutMillis);
        });
    }

    /** Discard the client for {@code nodeId} (after a transport failure). */
    public void invalidate(String nodeId) {
        var client = clients.remove(nodeId);
        if (client != null) {
            client.close();
        }
    }

    /** Drain in-flight requests up to {@code drainMillis}, then close every connection. */
    public void close(long drainMillis) {
        closed = true;
        long deadline = System.currentTimeMillis() + drainMillis;
        while (System.currentTimeMillis() < deadline
                && clients.values().stream().anyMatch(c -> c.isAlive() && c.pendingCount() > 0)) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        int abandoned = clients.values().stream().mapToInt(ForwardingClient::pendingCount).sum();
        if (abandoned > 0) {
            LOGGER.warn("closing forwarding pool with {} in-flight request(s) — they fail as transport errors",
                    abandoned);
        }
        clients.values().forEach(ForwardingClient::close);
        clients.clear();
    }
}
