package org.kendar.cqrses.cluster;

import org.kendar.cqrses.cluster.forwarding.BusRemoteCommandExecutor;
import org.kendar.cqrses.cluster.forwarding.ClusterCommandForwarder;
import org.kendar.cqrses.cluster.forwarding.ClusterRoutingTable;
import org.kendar.cqrses.cluster.forwarding.CommandForwardingServer;
import org.kendar.cqrses.cluster.forwarding.ForwardingClientPool;
import org.kendar.cqrses.cluster.forwarding.RemoteCommandExecutor;
import org.kendar.cqrses.cluster.spi.CommandForwarder;
import org.kendar.cqrses.db.Db;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrator for a single kf-cluster node. Build it with {@link ClusterNodeBuilder}, then
 * {@link #start(int, int, ItemProcessor)} to join the cluster: it initialises the schema,
 * seeds/validates the shared {@code N}, registers its node row, and brings up the liveness server,
 * heartbeat, worker, and leader services (all on plain {@code java.util.concurrent} daemon threads —
 * never the framework {@code Scheduler}).
 * <p>
 * kf-cluster's sole guarantee is that <b>exactly one live node runs {@code process(i)} at a time</b>.
 * The application calls {@link #release(int)} once it has finished winding a lost partition down.
 *
 * @see ItemProcessor
 */
public class ClusterNode {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterNode.class.getName());

    private final Db db;
    private final String nodeId;
    private final String host;
    private final ClusterClock clock;
    private final LeaderLock leaderLock;
    private final int forwardPort;
    private final RemoteCommandExecutor remoteCommandExecutor;

    private Liveness liveness;
    private HeartbeatService heartbeat;
    private WorkerService worker;
    private LeaderService leader;
    private CommandForwardingServer forwardingServer;
    private ClusterRoutingTable routingTable;
    private ForwardingClientPool clientPool;
    private ClusterCommandForwarder forwarder;

    ClusterNode(Db db, String nodeId, String host, ClusterClock clock, LeaderLock leaderLock, int forwardPort) {
        this(db, nodeId, host, clock, leaderLock, forwardPort, null);
    }

    ClusterNode(Db db, String nodeId, String host, ClusterClock clock, LeaderLock leaderLock,
                int forwardPort, RemoteCommandExecutor remoteCommandExecutor) {
        this.db = db;
        this.nodeId = nodeId;
        this.host = host;
        this.clock = clock;
        this.leaderLock = leaderLock;
        this.forwardPort = forwardPort;
        this.remoteCommandExecutor = remoteCommandExecutor;
    }

    public static ClusterNodeBuilder builder() {
        return new ClusterNodeBuilder();
    }

    public String nodeId() {
        return nodeId;
    }

    /**
     * Join the cluster and start processing. {@code itemCount} (the partition count {@code N}) must
     * be supplied identically to every node — a node started with a different {@code N} than the
     * seeded value refuses to start. {@code livenessPort} is the same on every node.
     *
     * @throws IllegalStateException if the local {@code itemCount} disagrees with the seeded value
     */
    public void start(int itemCount, int livenessPort, ItemProcessor processor) {
        ClusterSchema.init(db);
        ClusterSchema.seedAndValidateItemCount(db, itemCount);

        if (forwardPort > 0) {
            // Bind BEFORE the heartbeat ever advertises the port: a peer that
            // reads our row must always find a listening socket behind it.
            forwardingServer = new CommandForwardingServer(forwardPort,
                    remoteCommandExecutor != null ? remoteCommandExecutor : new BusRemoteCommandExecutor());
            forwardingServer.start();
            routingTable = new ClusterRoutingTable(db, nodeId);
            routingTable.start();
            clientPool = new ForwardingClientPool(ClusterConfig.FORWARD_CONNECT_TIMEOUT);
            forwarder = new ClusterCommandForwarder(routingTable, clientPool);
        }

        worker = new WorkerService(db, nodeId, clock, processor);
        liveness = new Liveness(livenessPort, worker::workerTick);
        liveness.start();

        heartbeat = new HeartbeatService(db, nodeId, host, livenessPort, forwardPort, clock);
        heartbeat.start();

        worker.start();

        leader = new LeaderService(db, nodeId, clock, leaderLock, itemCount);
        leader.start();

        LOGGER.info("cluster node {} started: N={}, livenessPort={}", nodeId, itemCount, livenessPort);
    }

    /**
     * App-facing callback: the application has finished winding partition {@code itemId} down (after
     * an {@link ItemProcessor#stopProcess(int)}). Only now does kf-cluster clear the lease, letting
     * the gaining node claim — which is what guarantees there is never a second pump.
     */
    public void release(int itemId) {
        if (worker != null) {
            worker.release(itemId);
        }
    }

    /**
     * Best-effort shutdown of every daemon service + the HTTP server, and release of the leader lock
     * if held. The crash path needs nothing explicit — the heartbeat simply goes stale. Each step is
     * isolated so one failure cannot block the others.
     */
    /**
     * The node's {@link CommandForwarder}, or {@code null} when forwarding is
     * disabled ({@code forwardPort == 0}) or the node has not started. The host
     * (e.g. {@code KfBootstrap}) installs it into kf-core's
     * {@code CommandForwarding} holder after {@code start(...)}.
     */
    public CommandForwarder commandForwarder() {
        return forwarder;
    }

    public void stop() {
        // Forwarding first: disable (late sends degrade to local), drain the
        // outbound futures, then drop the server — its in-flight inbound
        // handlers finish against buses that stop only after this returns.
        if (forwarder != null) {
            stopQuietly("forwarder", forwarder::disable);
        }
        if (clientPool != null) {
            stopQuietly("forwardClients", () -> clientPool.close(ClusterConfig.FORWARD_DRAIN));
        }
        stopQuietly("forwardServer", forwardingServer == null ? null : forwardingServer::stop);
        stopQuietly("routingTable", routingTable == null ? null : routingTable::stop);
        stopQuietly("leader", leader == null ? null : leader::stop);
        stopQuietly("worker", worker == null ? null : worker::stop);
        stopQuietly("heartbeat", heartbeat == null ? null : heartbeat::stop);
        stopQuietly("liveness", liveness == null ? null : liveness::stop);
        stopQuietly("leaderLock", leaderLock == null ? null : leaderLock::release);
    }

    private void stopQuietly(String what, Runnable action) {
        try {
            if (action != null) {
                action.run();
            }
        } catch (RuntimeException e) {
            LOGGER.warn("error stopping {}: {}", what, e.getMessage());
        }
    }
}
