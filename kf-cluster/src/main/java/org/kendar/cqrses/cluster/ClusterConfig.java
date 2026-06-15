package org.kendar.cqrses.cluster;

/**
 * Named timing constants for kf-cluster. All durations are epoch-millisecond spans
 * (stored times are {@code BIGINT} epoch millis), so every staleness/lease comparison
 * is a plain {@code long} subtraction with no timezone or dialect surprise.
 * <p>
 * The margins are intentionally wide so a loosely-synced (NTP) clock or a transient GC
 * pause cannot trip a false death: a {@code 9s} staleness window against a {@code 3s}
 * heartbeat, a {@code 30s} lease against a {@code 10s} renew. See {@code docs/tricks.md}.
 */
public final class ClusterConfig {

    private ClusterConfig() {
    }

    /** How often a node re-stamps its {@code cluster_nodes.last_heartbeat}. */
    public static final long HEARTBEAT = 3_000L;

    /** A node not seen within this window is a candidate for death (subject to /alive probe). */
    public static final long STALENESS_WINDOW = 9_000L;

    /** A GC pause longer than this triggers an immediate out-of-band heartbeat re-upsert. */
    public static final long GC_PAUSE_THRESHOLD = 4_000L;

    /** Leader reconcile cadence. */
    public static final long LEADER_TICK = 5_000L;

    /** Worker reconcile cadence. */
    public static final long WORKER_TICK = 5_000L;

    /** Processing lease lifetime; a lease older than this is reclaimable by a new owner. */
    public static final long LEASE = 30_000L;

    /** Worker lease-renew cadence (well under {@link #LEASE}). */
    public static final long LEASE_RENEW = 10_000L;

    /** A membership change must persist this long before the leader rebalances. */
    public static final long MEMBERSHIP_STABILIZE = 10_000L;

    /** Continuous instability for this long forces a rebalance over continuously-present nodes. */
    public static final long MAX_INSTABILITY = 30_000L;

    /** Per-request timeout for the leader's {@code GET /alive} dead-node confirmation probe. */
    public static final long LIVENESS_TIMEOUT = 2_000L;

    /** Consecutive failed {@code /alive} probes before a stale node is confirmed dead. */
    public static final int LIVENESS_FAIL_TICKS = 2;

    /** Leader-lock lease lifetime; an unrenewed leader is supplantable after this. */
    public static final long LEADER_LOCK_LEASE = 15_000L;

    /** External-monitor staleness threshold on {@code cluster_leader_health.last_tick} (docs only). */
    public static final long MONITOR_STALE = 15_000L;

    /** Dead {@code cluster_nodes} rows staler than this are pruned by the leader. */
    public static final long NODE_GC_STALE = 60_000L;

    /** Periodic refresh cadence of the command-forwarding routing table. */
    public static final long ROUTING_REFRESH = 5_000L;

    /** Minimum gap between on-miss/on-failure routing refreshes (stampede guard). */
    public static final long ROUTING_REFRESH_MIN_GAP = 500L;

    /** TCP connect timeout towards a peer's forwarding server. */
    public static final long FORWARD_CONNECT_TIMEOUT = 2_000L;

    /** Wait-mode (sendSync) budget for the remote handler's response. */
    public static final long FORWARD_SYNC_TIMEOUT = 30_000L;

    /** Ack-mode (async send) budget for the owner's receipt confirmation. */
    public static final long FORWARD_ACK_TIMEOUT = 5_000L;

    /** Shutdown budget for draining in-flight outbound forwards before failing them. */
    public static final long FORWARD_DRAIN = 5_000L;
}
