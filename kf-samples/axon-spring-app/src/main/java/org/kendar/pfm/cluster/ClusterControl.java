package org.kendar.pfm.cluster;

import org.axonframework.config.Configuration;
import org.axonframework.eventhandling.EventProcessor;
import org.axonframework.eventhandling.StreamingEventProcessor;
import org.kendar.pfm.config.PfmProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.TreeSet;

/**
 * Runtime control over <i>this node's</i> cluster participation, exposed so a test (or operator) can
 * stop and restart it <b>without killing the JVM</b> — the same contract as the kf sample's
 * {@code ClusterControl}, but expressed in Axon terms.
 *
 * <p>In server-less Axon "cluster participation" == this node's streaming event processors holding
 * token-store segment claims. So {@link #stop()} shuts the processors down (releasing every claim, so
 * peers steal those segments — the analog of kf releasing segment ownership) while the JVM, HTTP
 * surface and command side stay up; {@link #start()} restarts them (reclaiming/stealing segments
 * back). The command side is unaffected either way (any node always handles commands, OCC at the
 * shared event store) — exactly as in kf, where the cluster governs only the event side.
 *
 * <p><b>No-op when not in cluster.</b> When {@code pfm.cluster.mode=false} (the single-node demo)
 * every operation here is an inert success reporting {@code enabled=false} and never touches a
 * processor — mirroring kf's "does nothing when not in cluster" requirement. (The processors still
 * run locally so the demo projects; the control surface simply doesn't expose them.)
 */
@Component
public class ClusterControl {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterControl.class);

    private final Configuration axonConfiguration;
    private final PfmProperties props;
    private final HeartbeatService heartbeat;

    private volatile boolean running;

    public ClusterControl(Configuration axonConfiguration, PfmProperties props,
                          HeartbeatService heartbeat) {
        this.axonConfiguration = axonConfiguration;
        this.props = props;
        this.heartbeat = heartbeat;
        // Axon auto-starts the processors at boot; when in cluster mode that means we begin "running".
        this.running = props.getCluster().isMode();
    }

    /**
     * Stop this node's cluster part: stop announcing presence (so peers shrink their membership and
     * raise their segment cap) and shut down every streaming processor (releasing its claims). The
     * survivors then steal the freed segments.
     */
    public synchronized Status stop() {
        if (props.getCluster().isMode() && running) {
            heartbeat.pause();
            streamingProcessors().forEach((name, p) -> {
                p.shutDown();
                LOGGER.trace("cluster part: stopped streaming processor '{}'", name);
            });
            running = false;
            LOGGER.trace("cluster part stopped via control API");
        }
        return status();
    }

    /**
     * (Re)start this node's cluster part: announce presence again (peers grow their membership and
     * lower their cap, releasing surplus) and start every streaming processor so it reclaims segments.
     */
    public synchronized Status start() {
        if (props.getCluster().isMode() && !running) {
            heartbeat.resume();
            streamingProcessors().forEach((name, p) -> {
                p.start();
                LOGGER.trace("cluster part: started streaming processor '{}'", name);
            });
            running = true;
            LOGGER.trace("cluster part started via control API");
        }
        return status();
    }

    public Status status() {
        boolean mode = props.getCluster().isMode();
        return new Status(mode, mode && running, nodeId(), props.getSegments());
    }

    /**
     * Segment ids this node currently owns (claims), unioned across the streaming processors. This is
     * the framework-honest ownership source the IT reads (one call per node) to build the
     * segment→owner map — robust to how Axon formats {@code token_entry.owner}.
     */
    public java.util.SortedSet<Integer> ownedSegments() {
        TreeSet<Integer> owned = new TreeSet<>();
        if (props.getCluster().isMode()) {
            streamingProcessors().values().forEach(p -> owned.addAll(p.processingStatus().keySet()));
        }
        return owned;
    }

    private Map<String, StreamingEventProcessor> streamingProcessors() {
        java.util.LinkedHashMap<String, StreamingEventProcessor> out = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, EventProcessor> e
                : axonConfiguration.eventProcessingConfiguration().eventProcessors().entrySet()) {
            if (e.getValue() instanceof StreamingEventProcessor sep) {
                out.put(e.getKey(), sep);
            }
        }
        return out;
    }

    private String nodeId() {
        String id = props.getCluster().getNodeId();
        return (id == null || id.isBlank()) ? null : id;
    }

    /** Immutable status snapshot serialised straight to JSON by the controller. */
    public record Status(boolean enabled, boolean running, String nodeId, int segments) {
    }
}
