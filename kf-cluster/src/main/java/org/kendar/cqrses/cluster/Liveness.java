package org.kendar.cqrses.cluster;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * JDK-only liveness + notify endpoint, bound to {@code livenessPort} (the same port on every
 * node). It holds <b>no reference to {@link org.kendar.cqrses.db.Db} or any app component</b> —
 * only a {@code Runnable} reconcile callback — so {@code GET /alive} keeps answering even while
 * the main app / GC / DB are stuck.
 *
 * <ul>
 *   <li>{@code GET /alive} → {@code 200 "alive"} from a fixed in-memory body.</li>
 *   <li>{@code POST /notify} → {@code 200} immediately, then the reconcile callback is dispatched
 *       to a <b>separate single-thread executor</b> so the HTTP layer never blocks and
 *       {@code /alive} stays responsive. The poke carries no authoritative payload; it just says
 *       "re-read the DB now".</li>
 * </ul>
 */
public class Liveness {

    private static final Logger LOGGER = LoggerFactory.getLogger(Liveness.class.getName());
    private static final byte[] ALIVE_BODY = "alive".getBytes(StandardCharsets.UTF_8);

    private final int port;
    private final Runnable onNotify;
    private final AtomicLong notifyCount = new AtomicLong();

    private HttpServer server;
    private ExecutorService httpExecutor;
    private ExecutorService notifyExecutor;

    public Liveness(int port, Runnable onNotify) {
        this.port = port;
        this.onNotify = onNotify;
    }

    public void start() {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new IllegalStateException("failed to bind liveness HTTP server on port " + port, e);
        }
        // The HttpServer's own executor only ever serves /alive and the immediate /notify ack —
        // never the reconcile work, which runs on notifyExecutor.
        httpExecutor = Executors.newSingleThreadExecutor(daemon("cluster-liveness-http"));
        notifyExecutor = Executors.newSingleThreadExecutor(daemon("cluster-liveness-notify"));
        server.setExecutor(httpExecutor);
        server.createContext("/alive", this::handleAlive);
        server.createContext("/notify", this::handleNotify);
        server.start();
    }

    private void handleAlive(HttpExchange ex) throws IOException {
        try (ex) {
            ex.sendResponseHeaders(200, ALIVE_BODY.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(ALIVE_BODY);
            }
        }
    }

    private void handleNotify(HttpExchange ex) throws IOException {
        try (ex) {
            ex.sendResponseHeaders(200, -1);
        }
        // Dispatch reconcile off the HTTP thread so /alive is never blocked behind it.
        notifyExecutor.submit(() -> {
            notifyCount.incrementAndGet();
            try {
                onNotify.run();
            } catch (RuntimeException e) {
                LOGGER.warn("notify reconcile failed: {}", e.getMessage());
            }
        });
    }

    /** Number of {@code /notify} pokes dispatched — test seam. */
    long notifyCount() {
        return notifyCount.get();
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
        }
        if (httpExecutor != null) {
            httpExecutor.shutdownNow();
        }
        if (notifyExecutor != null) {
            notifyExecutor.shutdownNow();
        }
    }

    private static java.util.concurrent.ThreadFactory daemon(String name) {
        return r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        };
    }
}
