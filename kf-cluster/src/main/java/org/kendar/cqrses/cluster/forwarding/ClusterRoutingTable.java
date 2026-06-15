package org.kendar.cqrses.cluster.forwarding;

import org.kendar.cqrses.cluster.ClusterConfig;
import org.kendar.cqrses.db.Db;
import org.kendar.cqrses.db.DbException;
import org.kendar.cqrses.observability.Observability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The sender-side routing cache: {@code segment → owner node → (host, forward_port)},
 * read from {@code cluster_assignments} + {@code cluster_nodes}. Two volatile
 * immutable maps swapped atomically per refresh — readers never see a half-built
 * table.
 * <p>
 * Refresh triggers: a periodic tick every {@link ClusterConfig#ROUTING_REFRESH}
 * (assignments only change on leader rebalances), plus {@link #refreshAsync()}
 * on a route miss or connection failure, rate-limited by
 * {@link ClusterConfig#ROUTING_REFRESH_MIN_GAP} so a burst of misses cannot
 * stampede the database. A DB error keeps the previous cache (same fail-safe
 * stance as the worker tick) — <b>stale routing is harmless</b>: the receiving
 * node executes regardless of current ownership and command-side OCC arbitrates,
 * so the worst case of staleness is exactly today's local-execution contention.
 * <p>
 * {@link #routeFor(int)} is empty when the owner is unknown, is this node, or
 * advertises {@code forward_port = 0} (forwarding disabled there — also the
 * mixed-version safety valve during rolling upgrades).
 */
public class ClusterRoutingTable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterRoutingTable.class.getName());

    private final Db db;
    private final String selfNodeId;
    private final AtomicLong lastRefresh = new AtomicLong(Long.MIN_VALUE);

    private volatile Map<Integer, String> ownerBySegment = Map.of();
    private volatile Map<String, NodeAddress> addressByNode = Map.of();

    private ScheduledExecutorService scheduler;

    public ClusterRoutingTable(Db db, String selfNodeId) {
        this.db = db;
        this.selfNodeId = selfNodeId;
    }

    public void start() {
        refreshNow();
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "kf-routing-refresh");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::refreshNow,
                ClusterConfig.ROUTING_REFRESH, ClusterConfig.ROUTING_REFRESH, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    /**
     * The forwarding endpoint of the node owning {@code segment}, or empty when
     * the command should run locally (owner unknown / self / not forwardable).
     */
    public Optional<NodeAddress> routeFor(int segment) {
        String owner = ownerBySegment.get(segment);
        if (owner == null || owner.equals(selfNodeId)) return Optional.empty();
        return Optional.ofNullable(addressByNode.get(owner));
    }

    /**
     * Schedule an out-of-band refresh (route miss / connection failure),
     * silently dropped when one ran within the last
     * {@link ClusterConfig#ROUTING_REFRESH_MIN_GAP} ms or the table is stopped.
     */
    public void refreshAsync() {
        long now = System.currentTimeMillis();
        long last = lastRefresh.get();
        if (now - last < ClusterConfig.ROUTING_REFRESH_MIN_GAP) return;
        if (!lastRefresh.compareAndSet(last, now)) return;
        var s = scheduler;
        if (s == null || s.isShutdown()) return;
        s.execute(this::refreshNow);
    }

    /** Synchronous refresh — package-visible test seam; DB errors keep the old cache. */
    void refreshNow() {
        long started = System.nanoTime();
        try {
            record Assignment(int itemId, String owner) {
            }
            var assignments = db.query(
                    "SELECT item_id, owner_node FROM cluster_assignments WHERE owner_node IS NOT NULL",
                    (rs, n) -> new Assignment(rs.getInt(1), rs.getString(2)));
            var nodes = db.query(
                    "SELECT node_id, host, forward_port FROM cluster_nodes WHERE forward_port > 0",
                    (rs, n) -> new NodeAddress(rs.getString(1), rs.getString(2), rs.getInt(3)));

            var owners = new HashMap<Integer, String>();
            assignments.forEach(a -> owners.put(a.itemId(), a.owner()));
            var addresses = new HashMap<String, NodeAddress>();
            nodes.forEach(a -> addresses.put(a.nodeId(), a));

            ownerBySegment = Map.copyOf(owners);
            addressByNode = Map.copyOf(addresses);
            lastRefresh.set(System.currentTimeMillis());
            Observability.get().onRoutingRefreshed(owners.size(), addresses.size(),
                    System.nanoTime() - started);
        } catch (DbException e) {
            LOGGER.debug("routing refresh swallowed (keeping previous cache): {}", e.getMessage());
        }
    }
}
