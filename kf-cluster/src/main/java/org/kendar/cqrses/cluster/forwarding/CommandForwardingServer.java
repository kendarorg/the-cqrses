package org.kendar.cqrses.cluster.forwarding;

import org.kendar.cqrses.observability.Observability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TCP server executing commands forwarded by peer nodes. Blocking sockets on
 * virtual threads (Java 25 — JEP 491 removed synchronized-pinning, so parking
 * inside the pipeline is carrier-safe): an accept-loop thread, one read-loop
 * thread per connection, and <b>one virtual thread per request</b> so a slow
 * handler (rehydration, OCC retries) never stalls the connection's read loop —
 * that per-request fan-out is what makes the single connection multiplexable.
 * Response writes are serialised by a per-connection lock.
 * <p>
 * {@code stop()} closes the listener and all connections; in-flight request
 * threads finish their handler work and fail harmlessly when writing to the
 * closed socket (the sender's timeout/transport handling owns that edge).
 */
public class CommandForwardingServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommandForwardingServer.class.getName());

    private final int requestedPort;
    private final RemoteCommandExecutor executor;
    private final Set<Socket> connections = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean running = new AtomicBoolean();

    private ServerSocket serverSocket;

    /** {@code port} may be 0 to bind an ephemeral port (tests); see {@link #port()}. */
    public CommandForwardingServer(int port, RemoteCommandExecutor executor) {
        this.requestedPort = port;
        this.executor = executor;
    }

    public synchronized void start() {
        if (running.get()) return;
        try {
            serverSocket = new ServerSocket(requestedPort);
        } catch (IOException e) {
            throw new IllegalStateException("cannot bind command-forwarding port " + requestedPort, e);
        }
        running.set(true);
        Thread.ofVirtual().name("kf-forward-accept").start(this::acceptLoop);
        LOGGER.info("command-forwarding server listening on port {}", port());
    }

    /** The actual bound port (== the requested one unless 0 was requested). */
    public int port() {
        return serverSocket == null ? requestedPort : serverSocket.getLocalPort();
    }

    private void acceptLoop() {
        while (running.get()) {
            try {
                Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                connections.add(socket);
                Thread.ofVirtual().name("kf-forward-conn-" + socket.getRemoteSocketAddress())
                        .start(() -> readLoop(socket));
            } catch (IOException e) {
                if (running.get()) {
                    LOGGER.warn("forwarding accept failed: {}", e.getMessage());
                }
            }
        }
    }

    private void readLoop(Socket socket) {
        try (socket) {
            var in = new DataInputStream(socket.getInputStream());
            var out = new DataOutputStream(socket.getOutputStream());
            var writeLock = new Object();
            while (running.get()) {
                WireCodec.CommandRequest request = WireCodec.readRequest(in);
                Thread.ofVirtual().name("kf-forward-req-" + request.requestId())
                        .start(() -> serve(request, out, writeLock));
            }
        } catch (EOFException | SocketException e) {
            // peer closed (or we are stopping) — normal connection end
        } catch (IOException e) {
            LOGGER.warn("forwarding connection dropped: {}", e.getMessage());
        } finally {
            connections.remove(socket);
        }
    }

    private void serve(WireCodec.CommandRequest request, DataOutputStream out, Object writeLock) {
        boolean sync = request.mode() == WireCodec.MODE_WAIT;
        long started = System.nanoTime();
        WireCodec.CommandResponse response;
        boolean ok = false;
        try {
            if (sync) {
                Object result = executor.executeSync(request.type(), request.aggregateVersion(), request.payload());
                response = result == null
                        ? WireCodec.CommandResponse.ack(request.requestId())
                        : WireCodec.CommandResponse.value(request.requestId(),
                        result.getClass().getName(), serializeResult(result));
            } else {
                executor.executeAck(request.type(), request.aggregateVersion(), request.payload());
                response = WireCodec.CommandResponse.ack(request.requestId());
            }
            ok = true;
        } catch (Exception e) {
            // Includes serialization failures of the result: the sender must hear
            // SOMETHING for this requestId or its sendSync hangs until timeout.
            response = WireCodec.CommandResponse.error(request.requestId(),
                    e.getClass().getName(), e.getMessage());
        }
        Observability.get().onForwardServed(request.type(), sync, System.nanoTime() - started, ok);
        try {
            synchronized (writeLock) {
                WireCodec.writeResponse(out, response);
            }
        } catch (IOException e) {
            LOGGER.debug("response write failed (peer gone?) for request {}: {}",
                    request.requestId(), e.getMessage());
        }
    }

    /** Serialization seam — overridable for tests; production uses the registered serializer. */
    protected byte[] serializeResult(Object result) {
        return org.kendar.cqrses.di.GlobalRegistry
                .get(org.kendar.cqrses.serialization.MessageSerializer.class)
                .serialize(result);
    }

    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) return;
        try {
            serverSocket.close();
        } catch (IOException ignored) {
            // closing anyway
        }
        for (Socket socket : connections) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // closing anyway
            }
        }
        connections.clear();
        LOGGER.info("command-forwarding server stopped");
    }
}
