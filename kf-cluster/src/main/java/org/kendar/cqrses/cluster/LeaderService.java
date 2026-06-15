package org.kendar.cqrses.cluster;

import org.kendar.cqrses.cluster.rows.AssignmentRow;
import org.kendar.cqrses.cluster.rows.NodeRow;
import org.kendar.cqrses.cluster.rows.RowMappers;
import org.kendar.cqrses.db.Db;
import org.kendar.cqrses.db.DbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Leader loop — runs only while the {@link LeaderLock} is held. Each tick it confirms leadership,
 * computes the live membership (heartbeat set, augmented by the {@code /alive} probe so a DB-blind
 * but alive node is spared), gates on membership stability, computes a minimal-movement assignment,
 * writes only the changed rows (each epoch-fenced), pokes the affected nodes, prunes dead node rows,
 * and stamps {@code cluster_leader_health}.
 * <p>
 * The HTTP probe / poke are behind overridable seams ({@link #probeAlive}, {@link #poke}) so leader
 * tests can stub a node responsive/dead without real sockets.
 * <p>
 * <b>Fail-safe:</b> any DB error aborts the tick — the lock lease expires and the cluster is briefly
 * leaderless (a stall, never a split brain).
 */
public class LeaderService {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderService.class.getName());

    private final Db db;
    private final String nodeId;
    private final ClusterClock clock;
    private final LeaderLock lock;
    private final int itemCount;

    // Stability-gate state — touched only from the single leader thread (or a test thread).
    private Set<String> lastLive;
    private long stableSince;
    private long instabilitySince;
    private Set<String> continuous;
    private final Map<String, Integer> probeFailures = new HashMap<>();

    private ScheduledExecutorService scheduler;

    public LeaderService(Db db, String nodeId, ClusterClock clock, LeaderLock lock, int itemCount) {
        this.db = db;
        this.nodeId = nodeId;
        this.clock = clock;
        this.lock = lock;
        this.itemCount = itemCount;
    }

    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cluster-leader");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::leaderTick,
                ClusterConfig.LEADER_TICK, ClusterConfig.LEADER_TICK, TimeUnit.MILLISECONDS);
    }

    /** One leader pass. Package-visible test seam; aborts the instant leadership is lost. */
    void leaderTick() {
        long epoch = lock.acquire();
        if (epoch < 0 || !lock.isHeld()) {
            return;
        }
        long now = clock.now();
        try {
            ClusterSchema.seedAndValidateItemCount(db, itemCount);

            List<NodeRow> nodes = db.query("SELECT * FROM cluster_nodes", RowMappers.NODE);
            Map<String, NodeRow> nodeById = new HashMap<>();
            Set<String> heartbeatLive = new HashSet<>();
            for (NodeRow nr : nodes) {
                nodeById.put(nr.nodeId(), nr);
                if (nr.lastHeartbeat() > now - ClusterConfig.STALENESS_WINDOW) {
                    heartbeatLive.add(nr.nodeId());
                }
            }

            List<AssignmentRow> assignments =
                    db.query("SELECT * FROM cluster_assignments", RowMappers.ASSIGNMENT);

            // Dead-node confirmation: probe owners that dropped out of the heartbeat set; a node
            // that still answers /alive is spared (this is what spares an alive-but-DB-blind node).
            Set<String> effectiveLive = deadNodeConfirmation(heartbeatLive, assignments, nodeById);

            // Stability gate over the effective live set.
            Set<String> members = stabilityGate(effectiveLive, now);
            if (members != null) {
                rebalance(epoch, now, members, assignments, nodeById);
            }

            prune(now);
            db.update("UPDATE cluster_leader_health SET last_tick = ?, epoch = ? WHERE id = 1", now, epoch);
        } catch (DbException e) {
            LOGGER.warn("leader tick aborted on DB error (stall, lease will expire): {}", e.getMessage());
        }
    }

    private Set<String> deadNodeConfirmation(Set<String> heartbeatLive,
                                             List<AssignmentRow> assignments,
                                             Map<String, NodeRow> nodeById) {
        Set<String> effective = new HashSet<>(heartbeatLive);
        // Clear stale failure counters for nodes that are heartbeat-live again.
        probeFailures.keySet().removeIf(heartbeatLive::contains);

        Set<String> ownersWithWork = new HashSet<>();
        for (AssignmentRow a : assignments) {
            if (a.ownerNode() != null) {
                ownersWithWork.add(a.ownerNode());
            }
        }
        for (String suspect : ownersWithWork) {
            if (heartbeatLive.contains(suspect)) {
                continue;
            }
            NodeRow nr = nodeById.get(suspect);
            if (nr == null) {
                continue; // no contact info; cannot spare, treat as dead
            }
            if (probeAlive(nr.host(), nr.livenessPort())) {
                probeFailures.remove(suspect);
                effective.add(suspect); // alive-but-DB-blind → spare
            } else {
                int fails = probeFailures.merge(suspect, 1, Integer::sum);
                if (fails < ClusterConfig.LIVENESS_FAIL_TICKS) {
                    effective.add(suspect); // not yet confirmed dead — spare for now
                }
                // else: confirmed dead — left out, its partitions become orphans
            }
        }
        return effective;
    }

    /**
     * @return the member set to rebalance over, or {@code null} to skip rebalancing this tick.
     */
    private Set<String> stabilityGate(Set<String> live, long now) {
        if (lastLive == null) {
            // Cold start: the first observation is immediately actionable (no prior membership).
            lastLive = new HashSet<>(live);
            stableSince = now - ClusterConfig.MEMBERSHIP_STABILIZE;
            instabilitySince = 0;
            continuous = new HashSet<>(live);
        } else if (!live.equals(lastLive)) {
            if (instabilitySince == 0) {
                instabilitySince = now;
                continuous = new HashSet<>(live);
            } else {
                continuous.retainAll(live);
            }
            lastLive = new HashSet<>(live);
            stableSince = now;
        } else if (instabilitySince != 0) {
            continuous.retainAll(live);
        }

        if (now - stableSince >= ClusterConfig.MEMBERSHIP_STABILIZE) {
            instabilitySince = 0;
            continuous = new HashSet<>(live);
            return live;
        }
        if (instabilitySince != 0 && now - instabilitySince >= ClusterConfig.MAX_INSTABILITY) {
            Set<String> forced = new HashSet<>(continuous);
            // Restart the episode so continued flapping rebalances at most once per MAX_INSTABILITY.
            instabilitySince = now;
            continuous = new HashSet<>(live);
            return forced;
        }
        return null;
    }

    private void rebalance(long epoch, long now, Set<String> members,
                           List<AssignmentRow> assignments, Map<String, NodeRow> nodeById) {
        Map<Integer, AssignmentRow> byItem = new HashMap<>();
        Map<Integer, String> current = new HashMap<>();
        for (AssignmentRow a : assignments) {
            byItem.put(a.itemId(), a);
            current.put(a.itemId(), a.ownerNode());
        }

        Map<Integer, String> target =
                Assignment.compute(itemCount, new ArrayList<>(members), current);
        if (target.isEmpty()) {
            return; // no members to assign to
        }

        Set<String> affected = new LinkedHashSet<>();
        for (int item = 0; item < itemCount; item++) {
            String want = target.get(item);
            if (want == null) {
                continue;
            }
            AssignmentRow row = byItem.get(item);
            if (row == null) {
                db.update("""
                        INSERT INTO cluster_assignments(item_id, owner_node, epoch, lease_holder, lease_until)
                        VALUES(?, ?, ?, NULL, NULL)
                        """, item, want, epoch);
                affected.add(want);
            } else if (!Objects.equals(row.ownerNode(), want)) {
                // Epoch fence: a stale leader's lower-epoch write is rejected.
                int changed = db.update("""
                        UPDATE cluster_assignments SET owner_node = ?, epoch = ?
                         WHERE item_id = ? AND epoch < ?
                        """, want, epoch, item, epoch);
                if (changed == 1) {
                    if (row.ownerNode() != null) {
                        affected.add(row.ownerNode());
                    }
                    affected.add(want);
                }
            }
        }

        for (String node : affected) {
            NodeRow nr = nodeById.get(node);
            if (nr != null) {
                poke(nr.host(), nr.livenessPort());
            }
        }
    }

    private void prune(long now) {
        db.update("DELETE FROM cluster_nodes WHERE last_heartbeat < ?", now - ClusterConfig.NODE_GC_STALE);
    }

    /**
     * {@code GET /alive} dead-node confirmation probe. Overridable test seam (template-method,
     * mirroring {@code Db.connection()}).
     */
    protected boolean probeAlive(String host, int port) {
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) URI.create("http://" + host + ":" + port + "/alive").toURL().openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout((int) ClusterConfig.LIVENESS_TIMEOUT);
            conn.setReadTimeout((int) ClusterConfig.LIVENESS_TIMEOUT);
            return conn.getResponseCode() == 200;
        } catch (IOException e) {
            return false;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    /**
     * Best-effort {@code POST /notify} poke (just "re-read the DB now"). Overridable test seam.
     */
    protected void poke(String host, int port) {
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) URI.create("http://" + host + ":" + port + "/notify").toURL().openConnection();
            conn.setRequestMethod("POST");
            conn.setConnectTimeout((int) ClusterConfig.LIVENESS_TIMEOUT);
            conn.setReadTimeout((int) ClusterConfig.LIVENESS_TIMEOUT);
            conn.getResponseCode();
        } catch (IOException e) {
            LOGGER.debug("poke {}:{} failed (best-effort): {}", host, port, e.getMessage());
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    public void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }
}
