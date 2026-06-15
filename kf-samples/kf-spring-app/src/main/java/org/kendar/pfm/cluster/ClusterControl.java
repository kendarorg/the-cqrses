package org.kendar.pfm.cluster;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.kendar.cqrses.cluster.ClusterNode;
import org.kendar.cqrses.cluster.ItemProcessor;
import org.kendar.cqrses.cluster.spi.CommandForwarding;
import org.kendar.cqrses.pg.SegmentCalculator;
import org.kendar.cqrses.pg.SegmentProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Runtime control over <i>this node's</i> cluster part, exposed so a test (or an operator) can stop
 * and restart cluster participation <b>without killing the JVM</b> — the container stays up, all
 * ports stay mapped, a debugger stays attached. It wraps the starter-provided {@link ClusterNode}
 * (DB-backed leader election + partition distribution) and {@link SegmentProcessor} (the event-side
 * pull pump).
 *
 * <p><b>No-op when not in cluster.</b> When {@code kf.cluster.enabled=false} the {@link ClusterNode},
 * {@link SegmentProcessor} and {@link ItemProcessor} beans are absent (the starter gates them on the
 * flag), so every operation here is an inert success reporting {@code enabled=false}. This satisfies
 * the "does nothing when not in cluster" contract: the endpoints are always present but touch
 * nothing.
 *
 * <p><b>Re-startability.</b> {@link ClusterNode#start} reconstructs its worker/heartbeat/leader/
 * liveness services on every call, the default {@code DbLeaderLock} is stateless and re-acquirable,
 * and {@link SegmentProcessor#stopAll()} followed by a fresh claim rebuilds the worker map — so a
 * {@link #stop()} then {@link #start()} on the same beans is safe.
 */
@Component
public class ClusterControl {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterControl.class);

    private final boolean enabled;
    private final int livenessPort;
    private final String configuredNodeId;
    private final ObjectProvider<ClusterNode> node;
    private final ObjectProvider<SegmentProcessor> segmentProcessor;
    private final ObjectProvider<ItemProcessor> itemProcessor;
    private final ObjectProvider<MeterRegistry> meterRegistry;

    /** Current state of this node's cluster part. Only ever true when {@link #enabled}. */
    private volatile boolean running;

    public ClusterControl(@Value("${kf.cluster.enabled:false}") boolean enabled,
                          @Value("${kf.liveness.port:8070}") int livenessPort,
                          @Value("${kf.cluster.node-id:}") String configuredNodeId,
                          ObjectProvider<ClusterNode> node,
                          ObjectProvider<SegmentProcessor> segmentProcessor,
                          ObjectProvider<ItemProcessor> itemProcessor,
                          ObjectProvider<MeterRegistry> meterRegistry) {
        this.enabled = enabled;
        this.livenessPort = livenessPort;
        this.configuredNodeId = configuredNodeId;
        this.node = node;
        this.segmentProcessor = segmentProcessor;
        this.itemProcessor = itemProcessor;
        this.meterRegistry = meterRegistry;
    }

    /**
     * The starter's {@code KfBootstrap} (a late-phase {@code SmartLifecycle}) has already started
     * the node at boot. {@code ApplicationReadyEvent} fires after that, so marking
     * {@code running = enabled} here makes the control's view match reality from the first request.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        this.running = enabled;
    }

    /**
     * Stop this node's cluster part. No-op (and reports {@code enabled=false}) when not in cluster.
     * Otherwise idempotent: stops the {@link ClusterNode} (worker/heartbeat/leader/liveness — which
     * also releases the leader lock and lets peers see this node leave) <b>and</b>
     * {@link SegmentProcessor#stopAll()} (halts the event-side pull pump and unparks the parked
     * {@code claimSegment} calls). Both are required — stopping the node alone would leave the pump
     * dispatching events that other nodes have taken over, duplicating work.
     */
    public synchronized Status stop() {
        if (enabled && running) {
            // Uninstall the forward hook BEFORE the node tears the transport down:
            // any send racing this stop degrades to plain local dispatch.
            CommandForwarding.reset();
            ClusterNode n = node.getIfAvailable();
            if (n != null) {
                n.stop();
            }
            SegmentProcessor sp = segmentProcessor.getIfAvailable();
            if (sp != null) {
                sp.stopAll();
            }
            running = false;
            LOGGER.trace("cluster part stopped via control API");
        }
        return status();
    }

    /**
     * (Re)start this node's cluster part. No-op (and reports {@code enabled=false}) when not in
     * cluster. Otherwise idempotent: {@link ClusterNode#start} re-joins, the worker tick re-claims
     * this node's assigned segments and the {@link SegmentProcessor} rebuilds its worker map.
     */
    public synchronized Status start() {
        if (enabled && !running) {
            ClusterNode n = node.getIfAvailable();
            ItemProcessor ip = itemProcessor.getIfAvailable();
            if (n != null && ip != null) {
                n.start(SegmentCalculator.getSegments(), livenessPort, ip);
                // Re-install the fresh forwarder (start() rebuilt it; the old one
                // was disabled by stop()). Null when forwarding is not configured.
                var forwarder = n.commandForwarder();
                if (forwarder != null) {
                    CommandForwarding.install(forwarder);
                }
                running = true;
                LOGGER.trace("cluster part started via control API");
            } else {
                LOGGER.warn("cannot start cluster part: ClusterNode or ItemProcessor bean absent");
            }
        }
        return status();
    }

    /** Current view: {@code enabled}, {@code running}, this node's id, and the segment count. */
    public Status status() {
        return new Status(enabled, enabled && running, nodeId(), SegmentCalculator.getSegments(),
                timerCount("kf.command.forward"), timerCount("kf.command.forward.serve"));
    }

    /** Total count across all tag permutations of one forwarding timer (0 when no registry). */
    private long timerCount(String meterName) {
        MeterRegistry registry = meterRegistry.getIfAvailable();
        if (registry == null) return 0;
        return registry.find(meterName).timers().stream().mapToLong(Timer::count).sum();
    }

    private String nodeId() {
        ClusterNode n = node.getIfAvailable();
        if (n != null) {
            return n.nodeId();
        }
        return (configuredNodeId == null || configuredNodeId.isBlank()) ? null : configuredNodeId;
    }

    /**
     * Immutable status snapshot serialised straight to JSON by the controller.
     * {@code forwardedCount} counts commands this node forwarded to peers;
     * {@code forwardServedCount} counts forwarded commands this node executed
     * for peers (both 0 when forwarding is disabled).
     */
    public record Status(boolean enabled, boolean running, String nodeId, int segments,
                         long forwardedCount, long forwardServedCount) {
    }
}
