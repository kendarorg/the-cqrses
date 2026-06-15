package org.kendar.cqrses.cluster.forwarding;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real server + real client over loopback on an ephemeral port — no Docker, no
 * mocks: the executor is a template-method stub per the repo testing strategy.
 */
class ForwardingLoopbackTest {

    private CommandForwardingServer server;
    private ForwardingClient client;

    /** Records calls; scripted per test. The server serializes results via this seam too. */
    private static class StubExecutor implements RemoteCommandExecutor {
        final List<String> syncTypes = new ArrayList<>();
        final List<String> ackTypes = new ArrayList<>();
        final List<Long> versions = new ArrayList<>();
        Object result;
        RuntimeException error;
        long delayMillis;

        @Override
        public synchronized Object executeSync(String typeName, long aggregateVersion, byte[] payload) {
            syncTypes.add(typeName);
            versions.add(aggregateVersion);
            sleep();
            if (error != null) throw error;
            return result;
        }

        @Override
        public synchronized void executeAck(String typeName, long aggregateVersion, byte[] payload) {
            ackTypes.add(typeName);
            versions.add(aggregateVersion);
            sleep();
            if (error != null) throw error;
        }

        private void sleep() {
            if (delayMillis > 0) {
                try {
                    Thread.sleep(delayMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /** Server whose result serialization is a plain toString — no GlobalRegistry needed. */
    private static CommandForwardingServer serverWith(RemoteCommandExecutor executor) {
        return new CommandForwardingServer(0, executor) {
            @Override
            protected byte[] serializeResult(Object result) {
                return result.toString().getBytes(StandardCharsets.UTF_8);
            }
        };
    }

    private void startPair(RemoteCommandExecutor executor) {
        server = serverWith(executor);
        server.start();
        client = new ForwardingClient("127.0.0.1", server.port(), 2_000);
    }

    @AfterEach
    void tearDown() {
        if (client != null) client.close();
        if (server != null) server.stop();
    }

    @Test
    void waitModeTransportsResultBack() throws Exception {
        var executor = new StubExecutor();
        executor.result = "the-result";
        startPair(executor);

        var response = client.send(WireCodec.MODE_WAIT, "RecordOperation", 7L,
                "{}".getBytes(StandardCharsets.UTF_8)).get(5, TimeUnit.SECONDS);

        assertEquals(WireCodec.STATUS_VALUE, response.status());
        assertEquals("java.lang.String", response.resultType());
        assertEquals("the-result", new String(response.resultPayload(), StandardCharsets.UTF_8));
        assertEquals(List.of("RecordOperation"), executor.syncTypes);
        assertEquals(List.of(7L), executor.versions);
    }

    @Test
    void waitModeVoidResultComesBackAsAck() throws Exception {
        var executor = new StubExecutor();
        executor.result = null;
        startPair(executor);

        var response = client.send(WireCodec.MODE_WAIT, "Cmd", -1L, new byte[0])
                .get(5, TimeUnit.SECONDS);
        assertEquals(WireCodec.STATUS_ACK, response.status());
    }

    @Test
    void waitModeRemoteExceptionComesBackAsError() throws Exception {
        var executor = new StubExecutor();
        executor.error = new IllegalStateException("insufficient funds");
        startPair(executor);

        var response = client.send(WireCodec.MODE_WAIT, "Cmd", -1L, new byte[0])
                .get(5, TimeUnit.SECONDS);
        assertEquals(WireCodec.STATUS_ERROR, response.status());
        assertEquals("java.lang.IllegalStateException", response.errorClass());
        assertEquals("insufficient funds", response.errorMessage());
    }

    @Test
    void ackModeConfirmsReceiptWithoutResult() throws Exception {
        var executor = new StubExecutor();
        startPair(executor);

        var response = client.send(WireCodec.MODE_ACK, "Cmd", -1L, new byte[0])
                .get(5, TimeUnit.SECONDS);
        assertEquals(WireCodec.STATUS_ACK, response.status());
        assertEquals(List.of("Cmd"), executor.ackTypes);
        assertTrue(executor.syncTypes.isEmpty());
    }

    @Test
    void hundredInterleavedRequestsMultiplexOnOneConnection() throws Exception {
        var executor = new RemoteCommandExecutor() {
            @Override
            public Object executeSync(String typeName, long aggregateVersion, byte[] payload) {
                // echo the version so each response is distinguishable; jitter the
                // completion order so correlation actually does the matching
                try {
                    Thread.sleep(aggregateVersion % 7);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return "r" + aggregateVersion;
            }

            @Override
            public void executeAck(String typeName, long aggregateVersion, byte[] payload) {
            }
        };
        startPair(executor);

        var futures = new ArrayList<CompletableFuture<WireCodec.CommandResponse>>();
        for (int i = 0; i < 100; i++) {
            futures.add(client.send(WireCodec.MODE_WAIT, "Cmd", i, new byte[0]));
        }
        for (int i = 0; i < 100; i++) {
            var response = futures.get(i).get(10, TimeUnit.SECONDS);
            assertEquals(WireCodec.STATUS_VALUE, response.status());
            assertEquals("r" + i, new String(response.resultPayload(), StandardCharsets.UTF_8),
                    "response " + i + " must match its request despite out-of-order completion");
        }
        assertEquals(0, client.pendingCount());
    }

    @Test
    void slowRequestDoesNotBlockLaterOnes() throws Exception {
        var firstStarted = new CountDownLatch(1);
        var executor = new RemoteCommandExecutor() {
            @Override
            public Object executeSync(String typeName, long aggregateVersion, byte[] payload) {
                if (aggregateVersion == 0) {
                    firstStarted.countDown();
                    try {
                        Thread.sleep(2_000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return "slow";
                }
                return "fast";
            }

            @Override
            public void executeAck(String typeName, long aggregateVersion, byte[] payload) {
            }
        };
        startPair(executor);

        var slow = client.send(WireCodec.MODE_WAIT, "Cmd", 0, new byte[0]);
        assertTrue(firstStarted.await(2, TimeUnit.SECONDS));
        var fast = client.send(WireCodec.MODE_WAIT, "Cmd", 1, new byte[0]);

        // The fast request must complete while the slow one is still parked.
        assertEquals("fast", new String(fast.get(1, TimeUnit.SECONDS).resultPayload(), StandardCharsets.UTF_8));
        assertFalse(slow.isDone(), "slow request should still be in flight");
        assertEquals("slow", new String(slow.get(5, TimeUnit.SECONDS).resultPayload(), StandardCharsets.UTF_8));
    }

    @Test
    void connectToClosedPortThrowsTransportException() {
        server = serverWith(new StubExecutor());
        server.start();
        int deadPort = server.port();
        server.stop();
        server = null;

        assertThrows(TransportException.class,
                () -> new ForwardingClient("127.0.0.1", deadPort, 500));
    }

    @Test
    void serverStopFailsPendingClientFutures() throws Exception {
        var executor = new StubExecutor();
        executor.delayMillis = 5_000;
        startPair(executor);

        var future = client.send(WireCodec.MODE_WAIT, "Cmd", -1L, new byte[0]);
        // wait until the request is actually parked server-side, then kill it
        Thread.sleep(200);
        server.stop();

        var ex = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        assertInstanceOf(TransportException.class, ex.getCause());
        assertFalse(client.isAlive(), "client must mark itself dead after the connection drops");
    }

    @Test
    void sendAfterCloseThrows() {
        startPair(new StubExecutor());
        client.close();
        assertThrows(TransportException.class,
                () -> client.send(WireCodec.MODE_WAIT, "Cmd", -1L, new byte[0]));
    }

    @Test
    void poolReplacesDeadClientAndDrainsOnClose() throws Exception {
        var executor = new StubExecutor();
        executor.result = "ok";
        startPair(executor);
        client.close();
        client = null;

        var pool = new ForwardingClientPool(2_000);
        var c1 = pool.clientFor("n2", "127.0.0.1", server.port());
        c1.close();
        var c2 = pool.clientFor("n2", "127.0.0.1", server.port());
        assertNotSame(c1, c2, "a dead client must be replaced on lookup");

        var response = c2.send(WireCodec.MODE_WAIT, "Cmd", -1L, new byte[0]).get(5, TimeUnit.SECONDS);
        assertEquals(WireCodec.STATUS_VALUE, response.status());

        pool.close(1_000);
        assertFalse(c2.isAlive());
        assertThrows(TransportException.class, () -> pool.clientFor("n2", "127.0.0.1", server.port()));
    }

    @Test
    void poolInvalidateDiscardsClient() {
        startPair(new StubExecutor());
        client.close();
        client = null;

        var pool = new ForwardingClientPool(2_000);
        var c1 = pool.clientFor("n2", "127.0.0.1", server.port());
        pool.invalidate("n2");
        assertFalse(c1.isAlive());
        var c2 = pool.clientFor("n2", "127.0.0.1", server.port());
        assertNotSame(c1, c2);
        pool.close(0);
    }
}
