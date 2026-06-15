package org.kendar.cqrses.cluster.forwarding;

/** A peer node's command-forwarding endpoint, read from {@code cluster_nodes}. */
public record NodeAddress(String nodeId, String host, int port) {
}
