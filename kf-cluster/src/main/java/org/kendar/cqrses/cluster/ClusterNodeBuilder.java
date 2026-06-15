package org.kendar.cqrses.cluster;

import org.kendar.cqrses.cluster.forwarding.RemoteCommandExecutor;
import org.kendar.cqrses.db.Db;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

/**
 * Builder for {@link ClusterNode}. The only required input is the {@link Db}. {@code nodeId}
 * defaults to a random UUID (an ephemeral node); pass a <b>stable</b> id to make a restart re-adopt
 * its identity and resume its partitions with minimal churn. {@code host} defaults to the local
 * address (overridable). {@code clock} and {@code leaderLock} are test/extension seams.
 */
public class ClusterNodeBuilder {

    private Db db;
    private String nodeId = UUID.randomUUID().toString();
    private String host = autoDetectHost();
    private ClusterClock clock = ClusterClock.SYSTEM;
    private LeaderLock leaderLock;
    private int forwardPort;
    private RemoteCommandExecutor remoteCommandExecutor;

    public ClusterNodeBuilder db(Db db) {
        this.db = db;
        return this;
    }

    public ClusterNodeBuilder nodeId(String nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    public ClusterNodeBuilder host(String host) {
        this.host = host;
        return this;
    }

    public ClusterNodeBuilder clock(ClusterClock clock) {
        this.clock = clock;
        return this;
    }

    /** Override the default DB-backed fencing lock (e.g. Ratis / ZooKeeper). */
    public ClusterNodeBuilder leaderLock(LeaderLock leaderLock) {
        this.leaderLock = leaderLock;
        return this;
    }

    /**
     * TCP port for the command-forwarding server. {@code 0} (the default) disables
     * forwarding entirely: no server is bound, the node advertises
     * {@code forward_port = 0} and peers never route commands to it.
     */
    public ClusterNodeBuilder forwardPort(int forwardPort) {
        this.forwardPort = forwardPort;
        return this;
    }

    /**
     * Override how a forwarded command is executed locally (test seam). Defaults
     * to {@code BusRemoteCommandExecutor} — the registered {@code CommandBus}'s
     * {@code sendSyncLocal} / {@code sendLocal} bypass entries.
     */
    public ClusterNodeBuilder remoteCommandExecutor(RemoteCommandExecutor remoteCommandExecutor) {
        this.remoteCommandExecutor = remoteCommandExecutor;
        return this;
    }

    public ClusterNode build() {
        if (db == null) {
            throw new IllegalStateException("db is required");
        }
        LeaderLock lock = leaderLock != null ? leaderLock : new DbLeaderLock(db, nodeId, clock);
        return new ClusterNode(db, nodeId, host, clock, lock, forwardPort, remoteCommandExecutor);
    }

    private static String autoDetectHost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "127.0.0.1";
        }
    }
}
