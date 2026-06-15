package org.kendar.cqrses.cluster.forwarding;

import org.kendar.cqrses.bus.Context;
import org.kendar.cqrses.cluster.ClusterConfig;
import org.kendar.cqrses.cluster.spi.CommandForwarder;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.exceptions.ForwardTimeoutException;
import org.kendar.cqrses.exceptions.RemoteCommandException;
import org.kendar.cqrses.observability.Observability;
import org.kendar.cqrses.pg.SegmentCalculator;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The kf-cluster implementation of the kf-core {@link CommandForwarder} SPI.
 * Routes a command to the node owning its segment; every "cannot forward"
 * branch degrades to local execution, which is always <i>correct</i> (per-JVM
 * aggregate lock + the {@code UNIQUE(aggregate_id, sequence)} backstop + the
 * OCC retry loop arbitrate cross-node overlap) — forwarding is a contention
 * optimisation plus read-your-writes for {@code sendSync}.
 *
 * <p>The one deliberate exception: once a request was <b>delivered</b> (the
 * frame write succeeded), a missing answer is ambiguous — the remote may have
 * executed — so Wait mode throws {@link ForwardTimeoutException} instead of
 * falling back (running locally could apply the command twice, and OCC does not
 * deduplicate semantically distinct appends), and Ack mode logs + counts but
 * never re-sends.
 *
 * <p>A connect/write failure (provably not delivered) gets exactly one retry
 * against a freshly-refreshed route — covers the common transient (peer
 * restarted, stale address) — then local fallback; burning longer on a remote
 * that local execution can replace inverts the optimisation trade.
 */
public class ClusterCommandForwarder implements CommandForwarder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterCommandForwarder.class.getName());

    private final ClusterRoutingTable routing;
    private final ForwardingClientPool pool;
    private volatile boolean enabled = true;

    public ClusterCommandForwarder(ClusterRoutingTable routing, ForwardingClientPool pool) {
        this.routing = routing;
        this.pool = pool;
    }

    /** Shutdown latch: after this every send degrades to local dispatch. */
    public void disable() {
        enabled = false;
    }

    @Override
    public Optional<ForwardResult> forwardSync(Object command, Context context) {
        var route = resolveRoute(context);
        if (route == null) return Optional.empty();

        long started = System.nanoTime();
        byte[] payload = serialize(command);
        CompletableFuture<WireCodec.CommandResponse> future =
                sendWithOneRetry(route, WireCodec.MODE_WAIT, context, payload);
        if (future == null) return Optional.empty();   // never delivered → local

        boolean ok = false;
        try {
            WireCodec.CommandResponse response =
                    await(future, ClusterConfig.FORWARD_SYNC_TIMEOUT, route, context);
            if (response.status() == WireCodec.STATUS_ERROR) {
                throw reconstruct(response.errorClass(), response.errorMessage(), route.nodeId());
            }
            Object value = response.status() == WireCodec.STATUS_VALUE
                    ? deserializeResult(response.resultType(), response.resultPayload())
                    : null;
            ok = true;
            return Optional.of(new ForwardResult(value));
        } finally {
            Observability.get().onCommandForwarded(context.getType(), route.nodeId(), true,
                    System.nanoTime() - started, ok);
        }
    }

    @Override
    public boolean forwardAsync(Object command, Context context) {
        var route = resolveRoute(context);
        if (route == null) return false;

        long started = System.nanoTime();
        byte[] payload = serialize(command);
        CompletableFuture<WireCodec.CommandResponse> future =
                sendWithOneRetry(route, WireCodec.MODE_ACK, context, payload);
        if (future == null) return false;              // never delivered → local

        boolean ok = false;
        try {
            await(future, ClusterConfig.FORWARD_ACK_TIMEOUT, route, context);
            ok = true;
        } catch (ForwardTimeoutException e) {
            // Delivered but unconfirmed: the owner may still process it, so a
            // local re-send risks a duplicate. Count it and move on — async
            // callers accepted at-least-once semantics already.
            LOGGER.warn("ack timeout forwarding {} to {} — not re-sending locally",
                    context.getType(), route.nodeId());
            Observability.get().onForwardFallback(context.getType(), "ack-timeout");
        } finally {
            Observability.get().onCommandForwarded(context.getType(), route.nodeId(), false,
                    System.nanoTime() - started, ok);
        }
        return true;
    }

    /** The peer to forward to, or {@code null} for "execute locally". */
    private NodeAddress resolveRoute(Context context) {
        if (!enabled) {
            Observability.get().onForwardFallback(context.getType(), "disabled");
            return null;
        }
        if (context.getAggregateId() == null) {
            // interceptor-style command with no aggregate identity — always local
            return null;
        }
        int segment = SegmentCalculator.calculateSegment(context.getAggregateId());
        var route = routing.routeFor(segment);
        if (route.isEmpty()) {
            routing.refreshAsync();
            Observability.get().onForwardFallback(context.getType(), "no-route");
            return null;
        }
        return route.get();
    }

    /**
     * Connect + frame-write with one refresh-and-retry on transport failure.
     * Returns {@code null} when the request was provably never delivered (both
     * attempts failed before/at the write) — the caller runs locally. A non-null
     * future means the frame left this node: from here on the outcome is owned
     * by {@link #await}.
     */
    private CompletableFuture<WireCodec.CommandResponse> sendWithOneRetry(
            NodeAddress firstRoute, byte mode, Context context, byte[] payload) {
        NodeAddress route = firstRoute;
        for (int attempt = 0; ; attempt++) {
            try {
                return transmit(route, mode, context.getType(), context.getAggregateVersion(), payload);
            } catch (TransportException e) {
                pool.invalidate(route.nodeId());
                routing.refreshAsync();
                if (attempt >= 1) {
                    LOGGER.debug("cannot reach {} for {} after retry — executing locally: {}",
                            route.nodeId(), context.getType(), e.getMessage());
                    Observability.get().onForwardFallback(context.getType(), "connect-failed");
                    return null;
                }
                int segment = SegmentCalculator.calculateSegment(context.getAggregateId());
                var refreshed = routing.routeFor(segment);
                if (refreshed.isEmpty()) {
                    Observability.get().onForwardFallback(context.getType(), "connect-failed");
                    return null;
                }
                route = refreshed.get();
            }
        }
    }

    private WireCodec.CommandResponse await(CompletableFuture<WireCodec.CommandResponse> future,
                                            long timeoutMillis, NodeAddress route, Context context) {
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new ForwardTimeoutException("no response from " + route.nodeId() + " within "
                    + timeoutMillis + "ms for " + context.getType() + " — outcome unknown, NOT re-running locally");
        } catch (ExecutionException e) {
            // Connection died after a successful write: same ambiguity as a timeout.
            throw new ForwardTimeoutException("connection to " + route.nodeId() + " lost awaiting "
                    + context.getType() + " — outcome unknown, NOT re-running locally", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ForwardTimeoutException("interrupted awaiting " + route.nodeId()
                    + " for " + context.getType(), e);
        }
    }

    /**
     * Rebuild the remote handler's exception when possible (same classpath on
     * every node: a {@code RuntimeException} subclass with a {@code (String)}
     * constructor), preserving the {@code sendSync} contract transparently;
     * otherwise wrap in {@link RemoteCommandException}.
     */
    private RuntimeException reconstruct(String errorClass, String message, String remoteNodeId) {
        try {
            Class<?> cls = Class.forName(errorClass);
            if (RuntimeException.class.isAssignableFrom(cls)) {
                return (RuntimeException) cls.getConstructor(String.class).newInstance(message);
            }
        } catch (ReflectiveOperationException ignored) {
            // fall through to the wrapper
        }
        return new RemoteCommandException(errorClass, message, remoteNodeId);
    }

    /** Transport seam (template-method for tests): connect via the pool and send one frame. */
    protected CompletableFuture<WireCodec.CommandResponse> transmit(
            NodeAddress route, byte mode, String type, long aggregateVersion, byte[] payload) {
        return pool.clientFor(route.nodeId(), route.host(), route.port())
                .send(mode, type, aggregateVersion, payload);
    }

    /** Serialization seam — production uses the registered serializer. */
    protected byte[] serialize(Object command) {
        return GlobalRegistry.get(MessageSerializer.class).serialize(command);
    }

    /** Result deserialization seam: the response carries the result's FQCN. */
    protected Object deserializeResult(String resultType, byte[] payload) {
        try {
            return GlobalRegistry.get(MessageSerializer.class)
                    .deserialize(payload, Class.forName(resultType));
        } catch (ClassNotFoundException e) {
            throw new RemoteCommandException(resultType,
                    "result class not on this node's classpath", "unknown");
        }
    }
}
