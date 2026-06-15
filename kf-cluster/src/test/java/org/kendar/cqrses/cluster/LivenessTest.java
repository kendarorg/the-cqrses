package org.kendar.cqrses.cluster;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LivenessTest {

    private Liveness liveness;
    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2)).build();

    @AfterEach
    void tearDown() {
        if (liveness != null) {
            liveness.stop();
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private HttpResponse<String> get(int port, String path) throws Exception {
        return client.send(HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(int port, String path) throws Exception {
        return client.send(HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + path))
                .POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void aliveAnswersWhileNotifyReconcileIsBlocked() throws Exception {
        CountDownLatch reconcileEntered = new CountDownLatch(1);
        CountDownLatch releaseReconcile = new CountDownLatch(1);
        AtomicBoolean reconcileFinished = new AtomicBoolean(false);

        int port = freePort();
        liveness = new Liveness(port, () -> {
            reconcileEntered.countDown();
            try {
                releaseReconcile.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            reconcileFinished.set(true);
        });
        liveness.start();

        // POST /notify returns 200 immediately even though the reconcile callback will block.
        assertEquals(200, post(port, "/notify").statusCode());
        assertTrue(reconcileEntered.await(2, TimeUnit.SECONDS), "reconcile should run off-thread");

        // While reconcile is blocked, GET /alive still answers 200 with the fixed body.
        HttpResponse<String> alive = get(port, "/alive");
        assertEquals(200, alive.statusCode());
        assertEquals("alive", alive.body());

        releaseReconcile.countDown();
        assertTrue(RecordingProcessor.await(reconcileFinished::get, 2000));
        assertEquals(1, liveness.notifyCount());
    }
}
