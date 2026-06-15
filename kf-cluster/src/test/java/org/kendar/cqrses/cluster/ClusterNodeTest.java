package org.kendar.cqrses.cluster;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end wiring/lifecycle of the {@link ClusterNode} orchestrator over a shared in-memory H2.
 * Behavioural guarantees are covered by the per-service tests; this asserts that {@code start}
 * brings the parts up (schema, node row, liveness endpoint, N validation) and {@code stop} is clean.
 */
class ClusterNodeTest extends ClusterTestBase {

    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private ClusterNode node;

    @AfterEach
    void tearDown() {
        if (node != null) {
            node.stop();
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @Test
    void startBringsUpSchemaNodeRowAndLiveness() throws Exception {
        int port = freePort();
        node = ClusterNode.builder().db(db).nodeId("N1").host("127.0.0.1").build();
        node.start(4, port, new RecordingProcessor());

        // Node row registered and N seeded.
        assertEquals(1, (long) db.queryForObject(
                "SELECT COUNT(*) FROM cluster_nodes WHERE node_id = 'N1'", Long.class));
        assertEquals(4, (int) db.queryForObject(
                "SELECT item_count FROM cluster_config WHERE id = 1", Integer.class));

        // Liveness endpoint answers.
        HttpResponse<String> alive = client.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + port + "/alive")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, alive.statusCode());
        assertEquals("alive", alive.body());
    }

    @Test
    void releaseAndStopAreSafeAndIdempotent() throws Exception {
        int port = freePort();
        node = ClusterNode.builder().db(db).nodeId("N1").build();
        node.start(4, port, new RecordingProcessor());

        assertDoesNotThrow(() -> node.release(0)); // releasing an unheld partition is a no-op
        assertDoesNotThrow(node::stop);
        assertDoesNotThrow(node::stop); // idempotent
    }

    @Test
    void nodeWithMismatchedNRefusesToStart() throws Exception {
        ClusterSchema.seedAndValidateItemCount(db, 8); // someone seeded N=8 first
        int port = freePort();
        node = ClusterNode.builder().db(db).nodeId("N2").build();

        assertThrows(IllegalStateException.class,
                () -> node.start(4, port, new RecordingProcessor()));
        node = null; // start failed; nothing to stop
    }

    @Test
    void builderDefaultsNodeIdToRandomUuid() {
        ClusterNode a = ClusterNode.builder().db(db).build();
        ClusterNode b = ClusterNode.builder().db(db).build();
        assertTrue(a.nodeId() != null && !a.nodeId().equals(b.nodeId()));
    }
}
