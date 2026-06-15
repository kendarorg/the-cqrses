package org.kendar.cqrses.cluster.spi;

import org.kendar.cqrses.bus.Context;
import org.kendar.cqrses.exceptions.ForwardTimeoutException;
import org.kendar.cqrses.exceptions.RemoteCommandException;

import java.util.Optional;

/**
 * kf-core SPI the cluster module implements to route a command to the node that
 * owns its segment. {@code CommandBus.sendSync} / {@code send} consult the
 * installed forwarder (see {@link CommandForwarding}) right after building the
 * {@code Context} and before any local dispatch; when the forwarder declines,
 * the command runs locally exactly as it does on a single node — local execution
 * is always <i>correct</i> (per-aggregate OCC plus the
 * {@code UNIQUE(aggregate_id, sequence)} backstop arbitrate cross-node overlap),
 * forwarding is a contention optimisation plus read-your-writes for
 * {@code sendSync}.
 * <p>
 * The dependency direction is preserved: kf-cluster → kf-core. kf-core never
 * imports kf-cluster.
 */
public interface CommandForwarder {

    /**
     * Wait mode ({@code sendSync}). Empty means "not forwarded — caller must
     * execute locally": forwarding disabled, {@code context.getAggregateId()} is
     * {@code null}, the owner is this node, the route is unknown, or the request
     * provably never reached the peer (connect/write failure). Present means the
     * remote handler ran; {@link ForwardResult#value()} is its return value
     * ({@code null} for {@code void} handlers).
     *
     * @throws RemoteCommandException  the remote handler threw — propagate to the
     *                                 sender, never re-run locally
     * @throws ForwardTimeoutException delivered but unanswered — the outcome is
     *                                 ambiguous, never re-run locally (the remote
     *                                 may have executed; OCC does not deduplicate
     *                                 semantically distinct appends)
     */
    Optional<ForwardResult> forwardSync(Object command, Context context);

    /**
     * Ack mode (async {@code send}). {@code true} means the owning node confirmed
     * receipt (handler failures then land in the owner's DLQ, exactly today's
     * async semantics); {@code false} means "not forwarded — caller must enqueue
     * locally".
     */
    boolean forwardAsync(Object command, Context context);

    /**
     * Wrapper distinguishing "remote handler returned {@code null}" from
     * "not forwarded" ({@code Optional.empty()}).
     */
    record ForwardResult(Object value) {
    }
}
