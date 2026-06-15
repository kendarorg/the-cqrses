package org.kendar.cqrses.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Bean-time configuration bound from the {@code kf.*} namespace. The two pre-context values
 * ({@code kf.segments}, {@code kf.liveness.port}) are also read directly from the
 * {@link org.springframework.core.env.Environment} by {@link KfSegmentEnvironmentPostProcessor}
 * because they are needed before {@code @ConfigurationProperties} binding occurs.
 */
@ConfigurationProperties("kf")
public class KfProperties {

    /** Partition / segment count. Mandatory via the post-processor; mirrored here for completeness. */
    private int segments = 3;

    private final Liveness liveness = new Liveness();
    private final Cluster cluster = new Cluster();
    private final Scan scan = new Scan();
    private final Observability observability = new Observability();

    /** Processing-group names that should receive the default policy on the {@code EventBus}. */
    private List<String> processingGroups = new ArrayList<>();

    public int getSegments() {
        return segments;
    }

    public void setSegments(int segments) {
        this.segments = segments;
    }

    public Liveness getLiveness() {
        return liveness;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public Scan getScan() {
        return scan;
    }

    public Observability getObservability() {
        return observability;
    }

    public List<String> getProcessingGroups() {
        return processingGroups;
    }

    public void setProcessingGroups(List<String> processingGroups) {
        this.processingGroups = processingGroups;
    }

    /** Liveness HTTP server. Port consumed only at cluster {@code start(...)}. */
    public static class Liveness {
        private int port = 8070;

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }
    }

    /** Cluster node configuration. Gated by {@link #enabled} (default {@code false}). */
    public static class Cluster {
        /** Off by default: single-node JDBC is the correct out-of-the-box mode (see plan caveat). */
        private boolean enabled = false;
        /** Stable id makes a restart re-adopt its identity; null → random UUID in the builder. */
        private String nodeId;
        /** Advertised host; null → auto-detect in the builder. */
        private String host;
        /**
         * Projection dispatch threads per processing group. {@code 1} (default) =
         * strictly one worker thread per group polling all this node's owned segments
         * in one wide read. A higher value fans dispatch across that many
         * segment-partitioned slots for CPU-heavy projections (per-aggregate ordering
         * still holds — a segment always maps to the same slot). See
         * {@code plans/singlePgForNode.md}.
         */
        private int dispatchConcurrency = 1;

        private final Forwarding forwarding = new Forwarding();

        public Forwarding getForwarding() {
            return forwarding;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public int getDispatchConcurrency() {
            return dispatchConcurrency;
        }

        public void setDispatchConcurrency(int dispatchConcurrency) {
            this.dispatchConcurrency = dispatchConcurrency;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
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

    /**
     * Command forwarding to the segment-owning node ({@code kf.cluster.forwarding.*}).
     * Off by default: when disabled (or when the owner's row advertises
     * {@code forward_port = 0}) every command executes locally exactly as before —
     * correct either way, forwarding only removes cross-node OCC contention and
     * gives {@code sendSync} read-your-writes against the owner.
     */
    public static class Forwarding {
        private boolean enabled = false;
        /** TCP port of this node's forwarding server; same on all nodes is fine (distinct hosts). */
        private int port = 8071;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }
    }

    /**
     * Framework-internal observability. When a {@code MeterRegistry} is on the
     * classpath and {@code enabled} (default), {@code KfObservabilityAutoConfiguration}
     * installs the Micrometer timers into {@code kf-core}'s {@code Observability}
     * holder. Set {@code kf.observability.enabled=false} to force the no-op.
     */
    public static class Observability {
        private boolean enabled = true;
        /**
         * Gates the extended bottleneck meters (append-phase split, in-flight /
         * pump-lag gauges, nudge counters) and the pump-lag sampler. Off by
         * default so the baseline meter set is unchanged unless opted in.
         */
        private boolean extendedMetrics = false;

        private final Trace trace = new Trace();
        private final Lag lag = new Lag();

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public boolean isExtendedMetrics() {
            return extendedMetrics;
        }

        public void setExtendedMetrics(boolean extendedMetrics) {
            this.extendedMetrics = extendedMetrics;
        }

        public Trace getTrace() {
            return trace;
        }

        public Lag getLag() {
            return lag;
        }

        /**
         * Sampled per-command end-to-end traces, kept in a bounded in-memory
         * ring (never persisted to the framework database — that would perturb
         * the measurements) and harvested via {@code GET /kf/perf-traces}.
         */
        public static class Trace {
            private boolean enabled = false;
            /** 1-in-N sampling; 1 = trace every command. */
            private int sampleEvery = 100;
            /** Ring capacity in traces; overflow drops the oldest (counted). */
            private int bufferCapacity = 10000;

            public boolean isEnabled() {
                return enabled;
            }

            public void setEnabled(boolean enabled) {
                this.enabled = enabled;
            }

            public int getSampleEvery() {
                return sampleEvery;
            }

            public void setSampleEvery(int sampleEvery) {
                this.sampleEvery = sampleEvery;
            }

            public int getBufferCapacity() {
                return bufferCapacity;
            }

            public void setBufferCapacity(int bufferCapacity) {
                this.bufferCapacity = bufferCapacity;
            }
        }

        /** Pull-pump backlog sampler (extended metrics only). {@code 0} = off. */
        public static class Lag {
            private long sampleIntervalMs = 5000;

            public long getSampleIntervalMs() {
                return sampleIntervalMs;
            }

            public void setSampleIntervalMs(long sampleIntervalMs) {
                this.sampleIntervalMs = sampleIntervalMs;
            }
        }
    }

    /** Classpath-scan roots for handler/aggregate/saga/projection discovery. */
    public static class Scan {
        /** Empty → fall back to Spring's auto-configuration packages. */
        private List<String> basePackages = new ArrayList<>();

        public List<String> getBasePackages() {
            return basePackages;
        }

        public void setBasePackages(List<String> basePackages) {
            this.basePackages = basePackages;
        }
    }
}
