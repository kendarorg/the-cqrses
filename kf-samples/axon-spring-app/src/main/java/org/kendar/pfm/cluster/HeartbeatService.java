package org.kendar.pfm.cluster;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.kendar.pfm.config.PfmProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Node-presence heartbeat backing the membership-aware segment cap (see {@code cluster_presence} in
 * {@code pfm-schema.sql}). The problem it solves: server-less Axon distributes work by token-store
 * claiming, but gives the application <b>no view of cluster membership</b> — and a cap derived from
 * who owns {@code token_entry} rows cannot bootstrap, because a node owning zero segments is invisible
 * to its peers (the first node to boot would claim all {@code segments} and never be told to release
 * any). So each cluster member independently beats a {@code cluster_presence} row while its cluster
 * part is "running", and {@link #dynamicCap()} sizes {@code maxClaimedSegments} to
 * {@code ceil(segments / liveNodes)}. The {@link AxonProcessorBalancingConfig} feeds that to both
 * pooled streaming processors.
 *
 * <p>This is the Axon-sample analog of the membership slice of kf's cluster module — the coordination
 * kf gets from its leader/heartbeat that open-source Axon offloads to Axon Server. Active only in
 * cluster mode; in the single-node demo {@link #dynamicCap()} returns {@code segments} (no cap) and no
 * heartbeat thread runs.
 *
 * <p>Membership reacts to all the ways a node can leave, well inside the token claim timeout
 * ({@code AxonTokenStoreConfig.CLAIM_TIMEOUT}, 10s):
 * <ul>
 *   <li><b>Graceful {@code /cluster/stop}</b> (JVM stays up): {@link #pause()} stops beating and
 *       deletes the row, so survivors see the smaller membership immediately.</li>
 *   <li><b>Container/JVM kill</b>: the row simply stops updating and goes stale after
 *       {@link #STALENESS_MS}; survivors then raise their cap and steal the dead node's expired
 *       claims.</li>
 * </ul>
 */
@Component
@DependsOn("readModelSchema") // ensure cluster_presence exists before the first beat
public class HeartbeatService {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatService.class);

    /** How often a running member refreshes its presence row. */
    static final long HEARTBEAT_INTERVAL_MS = 2_000L;

    /** A presence row older than this is treated as gone. Kept below the token claim timeout so
     *  membership shrinks before/around the moment a dead node's claims become stealable. */
    static final long STALENESS_MS = 7_000L;

    private final DataSource dataSource;
    private final PfmProperties props;

    private volatile ScheduledExecutorService beat;
    private volatile boolean beating;

    public HeartbeatService(DataSource dataSource, PfmProperties props) {
        this.dataSource = dataSource;
        this.props = props;
    }

    @PostConstruct
    public void startIfClustered() {
        if (!props.getCluster().isMode()) {
            return; // single-node demo: no presence, no cap
        }
        beating = true;
        beat = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "presence-heartbeat");
            t.setDaemon(true);
            return t;
        });
        beat.scheduleAtFixedRate(this::beatOnce, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        LOGGER.trace("presence heartbeat started for node '{}'", nodeId());
    }

    /** Stop announcing this node and drop its presence row (graceful leave via the control API). */
    public synchronized void pause() {
        beating = false;
        deletePresence();
        LOGGER.trace("presence heartbeat paused for node '{}'", nodeId());
    }

    /** Resume announcing this node, beating one row immediately so peers see it without delay. */
    public synchronized void resume() {
        beating = true;
        beatOnce();
        LOGGER.trace("presence heartbeat resumed for node '{}'", nodeId());
    }

    /**
     * Max segments this node may claim per pooled processor: {@code ceil(segments / liveNodes)} in
     * cluster mode, or {@code segments} (effectively uncapped) in the single-node demo. Re-read by
     * Axon's coordinator every cycle, so the cap rises automatically as peers leave and falls as they
     * (re)join.
     */
    public int dynamicCap() {
        int segments = props.getSegments();
        if (!props.getCluster().isMode()) {
            return segments;
        }
        int live = Math.max(1, liveNodeCount());
        return (segments + live - 1) / live; // ceil
    }

    /** Members with a presence row refreshed within {@link #STALENESS_MS}. */
    public int liveNodeCount() {
        long cutoff = System.currentTimeMillis() - STALENESS_MS;
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT COUNT(*) FROM cluster_presence WHERE last_seen > ?")) {
            ps.setLong(1, cutoff);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        } catch (SQLException e) {
            // On a transient DB hiccup, assume we are alone rather than 0 (which would divide-by-zero
            // guard to a cap of `segments`); a momentary over-claim is self-corrected next cycle.
            LOGGER.debug("presence read failed, treating membership as 1: {}", e.getMessage());
            return 1;
        }
    }

    private void beatOnce() {
        if (!beating) {
            return;
        }
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO cluster_presence (node_id, last_seen) VALUES (?, ?) "
                             + "ON DUPLICATE KEY UPDATE last_seen = VALUES(last_seen)")) {
            ps.setString(1, nodeId());
            ps.setLong(2, System.currentTimeMillis());
            ps.executeUpdate();
        } catch (SQLException e) {
            LOGGER.debug("presence beat failed for node '{}': {}", nodeId(), e.getMessage());
        }
    }

    private void deletePresence() {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "DELETE FROM cluster_presence WHERE node_id = ?")) {
            ps.setString(1, nodeId());
            ps.executeUpdate();
        } catch (SQLException e) {
            LOGGER.debug("presence delete failed for node '{}': {}", nodeId(), e.getMessage());
        }
    }

    private String nodeId() {
        String id = props.getCluster().getNodeId();
        return (id == null || id.isBlank()) ? "unknown" : id;
    }

    @PreDestroy
    public void shutdown() {
        beating = false;
        if (beat != null) {
            beat.shutdownNow();
        }
        deletePresence();
    }
}
