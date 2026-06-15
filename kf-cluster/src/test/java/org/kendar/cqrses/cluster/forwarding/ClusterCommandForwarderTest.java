package org.kendar.cqrses.cluster.forwarding;

import org.junit.jupiter.api.Test;
import org.kendar.cqrses.bus.Context;
import org.kendar.cqrses.cluster.ClusterTestBase;
import org.kendar.cqrses.exceptions.ForwardTimeoutException;
import org.kendar.cqrses.exceptions.RemoteCommandException;
import org.kendar.cqrses.pg.SegmentCalculator;
import org.kendar.cqrses.utils.UUIDGenerator;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The full §decision-tree of the forwarder against a stubbed transport (the
 * {@code transmit} template method) and the real routing table on H2 — no
 * sockets, no GlobalRegistry.
 */
class ClusterCommandForwarderTest extends ClusterTestBase {

    private static final String SELF = "n1";
    private static final String PEER = "n2";

    private UUID aggregateId;
    private int segment;

    /** Transport script: each entry is either a future or a TransportException to throw. */
    private final List<Object> transmissions = new ArrayList<>();
    private final List<NodeAddress> attempted = new ArrayList<>();

    private ClusterCommandForwarder forwarder(ClusterRoutingTable routing) {
        return new ClusterCommandForwarder(routing, new ForwardingClientPool(100)) {
            @Override
            protected CompletableFuture<WireCodec.CommandResponse> transmit(
                    NodeAddress route, byte mode, String type, long aggregateVersion, byte[] payload) {
                attempted.add(route);
                Object next = transmissions.remove(0);
                if (next instanceof TransportException te) throw te;
                @SuppressWarnings("unchecked")
                var future = (CompletableFuture<WireCodec.CommandResponse>) next;
                return future;
            }

            @Override
            protected byte[] serialize(Object command) {
                return command.toString().getBytes(StandardCharsets.UTF_8);
            }

            @Override
            protected Object deserializeResult(String resultType, byte[] payload) {
                return new String(payload, StandardCharsets.UTF_8);
            }
        };
    }

    private ClusterRoutingTable routedToPeer() {
        aggregateId = UUIDGenerator.newUuid();
        segment = SegmentCalculator.calculateSegment(aggregateId);
        db.insertInto("cluster_nodes")
                .set("node_id", PEER).set("host", "10.0.0.2")
                .set("liveness_port", 8070).set("forward_port", 8071)
                .set("last_heartbeat", clock.now()).execute();
        db.insertInto("cluster_assignments")
                .set("item_id", segment).set("owner_node", PEER).set("epoch", 1L).execute();
        var routing = new ClusterRoutingTable(db, SELF);
        routing.refreshNow();
        return routing;
    }

    private Context contextFor(UUID id) {
        var context = new Context();
        context.setType("RecordOperation");
        context.setAggregateId(id);
        context.setAggregateVersion(-1L);
        return context;
    }

    // --- forwardSync -------------------------------------------------------------

    @Test
    void valueResponseComesBackDeserialized() {
        var fwd = forwarder(routedToPeer());
        transmissions.add(CompletableFuture.completedFuture(WireCodec.CommandResponse.value(
                1L, "java.lang.String", "result!".getBytes(StandardCharsets.UTF_8))));

        var out = fwd.forwardSync("cmd", contextFor(aggregateId));

        assertTrue(out.isPresent());
        assertEquals("result!", out.get().value());
        assertEquals(List.of(PEER), attempted.stream().map(NodeAddress::nodeId).toList());
    }

    @Test
    void ackResponseMeansVoidResult() {
        var fwd = forwarder(routedToPeer());
        transmissions.add(CompletableFuture.completedFuture(WireCodec.CommandResponse.ack(1L)));

        var out = fwd.forwardSync("cmd", contextFor(aggregateId));
        assertTrue(out.isPresent());
        assertNull(out.get().value());
    }

    @Test
    void errorResponseReconstructsOriginalExceptionType() {
        var fwd = forwarder(routedToPeer());
        transmissions.add(CompletableFuture.completedFuture(WireCodec.CommandResponse.error(
                1L, "java.lang.IllegalStateException", "insufficient funds")));

        var thrown = assertThrows(IllegalStateException.class,
                () -> fwd.forwardSync("cmd", contextFor(aggregateId)));
        assertEquals("insufficient funds", thrown.getMessage());
    }

    @Test
    void unreconstructableErrorWrapsInRemoteCommandException() {
        var fwd = forwarder(routedToPeer());
        transmissions.add(CompletableFuture.completedFuture(WireCodec.CommandResponse.error(
                1L, "com.acme.NoSuchExceptionClass", "boom")));

        var thrown = assertThrows(RemoteCommandException.class,
                () -> fwd.forwardSync("cmd", contextFor(aggregateId)));
        assertEquals("com.acme.NoSuchExceptionClass", thrown.getRemoteExceptionClass());
        assertEquals(PEER, thrown.getRemoteNodeId());
    }

    @Test
    void nullAggregateIdNeverForwards() {
        var fwd = forwarder(routedToPeer());
        assertTrue(fwd.forwardSync("cmd", contextFor(null)).isEmpty());
        assertTrue(attempted.isEmpty());
    }

    @Test
    void unassignedSegmentFallsBackLocal() {
        var routing = new ClusterRoutingTable(db, SELF);
        routing.refreshNow();
        var fwd = forwarder(routing);

        assertTrue(fwd.forwardSync("cmd", contextFor(UUIDGenerator.newUuid())).isEmpty());
        assertTrue(attempted.isEmpty());
    }

    @Test
    void selfOwnedSegmentFallsBackLocal() {
        aggregateId = UUIDGenerator.newUuid();
        segment = SegmentCalculator.calculateSegment(aggregateId);
        db.insertInto("cluster_assignments")
                .set("item_id", segment).set("owner_node", SELF).set("epoch", 1L).execute();
        var routing = new ClusterRoutingTable(db, SELF);
        routing.refreshNow();
        var fwd = forwarder(routing);

        assertTrue(fwd.forwardSync("cmd", contextFor(aggregateId)).isEmpty());
        assertTrue(attempted.isEmpty());
    }

    @Test
    void disabledForwarderFallsBackLocal() {
        var fwd = forwarder(routedToPeer());
        fwd.disable();
        assertTrue(fwd.forwardSync("cmd", contextFor(aggregateId)).isEmpty());
        assertTrue(attempted.isEmpty());
    }

    @Test
    void transportFailureRetriesOnceThenFallsBackLocal() {
        var fwd = forwarder(routedToPeer());
        transmissions.add(new TransportException("connect refused"));
        transmissions.add(new TransportException("connect refused again"));

        assertTrue(fwd.forwardSync("cmd", contextFor(aggregateId)).isEmpty());
        assertEquals(2, attempted.size(), "exactly one retry after the first transport failure");
    }

    @Test
    void transportFailureThenSuccessOnRetry() {
        var fwd = forwarder(routedToPeer());
        transmissions.add(new TransportException("connect refused"));
        transmissions.add(CompletableFuture.completedFuture(WireCodec.CommandResponse.ack(2L)));

        var out = fwd.forwardSync("cmd", contextFor(aggregateId));
        assertTrue(out.isPresent());
        assertEquals(2, attempted.size());
    }

    @Test
    void connectionLostAfterDeliveryIsAmbiguousNeverLocal() {
        var fwd = forwarder(routedToPeer());
        var dead = new CompletableFuture<WireCodec.CommandResponse>();
        dead.completeExceptionally(new TransportException("connection lost"));
        transmissions.add(dead);

        assertThrows(ForwardTimeoutException.class,
                () -> fwd.forwardSync("cmd", contextFor(aggregateId)));
        assertEquals(1, attempted.size(), "post-delivery loss must NOT retry — duplicate hazard");
    }

    // --- forwardAsync ------------------------------------------------------------

    @Test
    void asyncAckReturnsTrue() {
        var fwd = forwarder(routedToPeer());
        transmissions.add(CompletableFuture.completedFuture(WireCodec.CommandResponse.ack(1L)));
        assertTrue(fwd.forwardAsync("cmd", contextFor(aggregateId)));
    }

    @Test
    void asyncNoRouteReturnsFalse() {
        var routing = new ClusterRoutingTable(db, SELF);
        routing.refreshNow();
        var fwd = forwarder(routing);
        assertFalse(fwd.forwardAsync("cmd", contextFor(UUIDGenerator.newUuid())));
    }

    @Test
    void asyncPostDeliveryLossStillReturnsTrueNoLocalResend() {
        var fwd = forwarder(routedToPeer());
        var dead = new CompletableFuture<WireCodec.CommandResponse>();
        dead.completeExceptionally(new TransportException("connection lost"));
        transmissions.add(dead);

        assertTrue(fwd.forwardAsync("cmd", contextFor(aggregateId)),
                "delivered-but-unconfirmed must not trigger a local duplicate");
    }

    @Test
    void asyncTransportFailureTwiceReturnsFalse() {
        var fwd = forwarder(routedToPeer());
        transmissions.add(new TransportException("refused"));
        transmissions.add(new TransportException("refused"));
        assertFalse(fwd.forwardAsync("cmd", contextFor(aggregateId)));
    }
}
