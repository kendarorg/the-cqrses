package org.kendar.cqrses.cluster.forwarding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One multiplexed connection to one peer's {@link CommandForwardingServer}.
 * {@link #send} frames the request under a write lock, parks a
 * {@link CompletableFuture} in the callback map keyed by the per-connection
 * monotonic {@code requestId}, and returns it; a single reader virtual thread
 * completes futures as responses arrive (any interleaving — the correlation id
 * does the matching). Any IO failure or {@link #close()} completes every
 * pending future exceptionally with {@link TransportException} and renders the
 * client dead ({@link #isAlive()} false) — the pool then discards it.
 */
public class ForwardingClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ForwardingClient.class.getName());

    private final Socket socket;
    private final DataOutputStream out;
    private final Object writeLock = new Object();
    private final AtomicLong requestIds = new AtomicLong();
    private final ConcurrentHashMap<Long, CompletableFuture<WireCodec.CommandResponse>> pending =
            new ConcurrentHashMap<>();
    private final AtomicBoolean alive = new AtomicBoolean(true);

    /** @throws TransportException when the peer cannot be reached within {@code connectTimeoutMillis}. */
    public ForwardingClient(String host, int port, long connectTimeoutMillis) {
        try {
            socket = new Socket();
            socket.connect(new InetSocketAddress(host, port), (int) connectTimeoutMillis);
            socket.setTcpNoDelay(true);
            out = new DataOutputStream(socket.getOutputStream());
            var in = new DataInputStream(socket.getInputStream());
            Thread.ofVirtual().name("kf-forward-client-" + host + ":" + port)
                    .start(() -> readLoop(in));
        } catch (IOException e) {
            throw new TransportException("cannot connect to " + host + ":" + port, e);
        }
    }

    /**
     * Send one command frame; the future completes with the peer's response, or
     * exceptionally with {@link TransportException} if the connection dies first.
     *
     * @throws TransportException when the write itself fails (request provably
     *                            not delivered — the caller may safely retry or
     *                            fall back to local execution)
     */
    public CompletableFuture<WireCodec.CommandResponse> send(byte mode, String type,
                                                             long aggregateVersion, byte[] payload) {
        if (!alive.get()) {
            throw new TransportException("connection already closed");
        }
        long requestId = requestIds.incrementAndGet();
        var future = new CompletableFuture<WireCodec.CommandResponse>();
        pending.put(requestId, future);
        try {
            synchronized (writeLock) {
                WireCodec.writeRequest(out,
                        new WireCodec.CommandRequest(mode, requestId, type, aggregateVersion, payload));
            }
        } catch (IOException e) {
            pending.remove(requestId);
            closeInternal(new TransportException("write failed", e));
            throw new TransportException("write failed", e);
        }
        return future;
    }

    private void readLoop(DataInputStream in) {
        try {
            while (alive.get()) {
                WireCodec.CommandResponse response = WireCodec.readResponse(in);
                var future = pending.remove(response.requestId());
                if (future != null) {
                    future.complete(response);
                } else {
                    LOGGER.debug("response for unknown/expired request {}", response.requestId());
                }
            }
        } catch (IOException e) {
            closeInternal(new TransportException("connection lost", e));
        }
    }

    public boolean isAlive() {
        return alive.get();
    }

    public int pendingCount() {
        return pending.size();
    }

    public void close() {
        closeInternal(new TransportException("connection closed"));
    }

    private void closeInternal(TransportException cause) {
        if (!alive.compareAndSet(true, false)) return;
        try {
            socket.close();
        } catch (IOException ignored) {
            // closing anyway
        }
        pending.forEach((id, future) -> future.completeExceptionally(cause));
        pending.clear();
    }
}
