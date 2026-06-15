package org.kendar.cqrses.cluster.rows;

/**
 * A {@code cluster_nodes} row: a node's stable identity, where to reach its liveness
 * endpoint, and the epoch-millis timestamp of its last heartbeat.
 */
public record NodeRow(String nodeId, String host, int livenessPort, long lastHeartbeat) {
}
