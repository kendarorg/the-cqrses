package org.kendar.pfm.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Framework-neutral knobs bound from the {@code pfm.*} namespace (kept separate from Axon's own
 * {@code axon.*} so they never collide). Mirrors the {@code kf.*} env-var shape the kf sample uses,
 * so the cluster IT can drive both apps with the same {@code PFM_*} container env vars.
 *
 * <ul>
 *   <li>{@code pfm.segments} — initial segment count per pooled streaming processor (the token-store
 *       distribution unit; matches kf's {@code kf.segments}).</li>
 *   <li>{@code pfm.cluster.mode} — whether this node participates as a cluster member; when false the
 *       {@code /cluster/*} control endpoints are inert no-ops (kf parity).</li>
 *   <li>{@code pfm.cluster.node-id} / {@code host} — this node's stable identity, surfaced in
 *       {@code /cluster/status} and used to map a token {@code owner} back to a node.</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "pfm")
public class PfmProperties {

    private int segments = 6;
    private Cluster cluster = new Cluster();

    public int getSegments() {
        return segments;
    }

    public void setSegments(int segments) {
        this.segments = segments;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public static class Cluster {
        private boolean mode;
        private String nodeId = "";
        private String host = "";

        public boolean isMode() {
            return mode;
        }

        public void setMode(boolean mode) {
            this.mode = mode;
        }

        public String getNodeId() {
            return nodeId;
        }

        public void setNodeId(String nodeId) {
            this.nodeId = nodeId;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }
    }
}
