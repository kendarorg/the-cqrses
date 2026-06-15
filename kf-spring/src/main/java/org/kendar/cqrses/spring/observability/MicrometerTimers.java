package org.kendar.cqrses.spring.observability;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.kendar.cqrses.observability.ObservabilityInterface;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Micrometer-backed {@link ObservabilityInterface}: turns each semantic hot-path
 * callback into a {@link Timer} or counter on the app's {@link MeterRegistry}.
 * Lives in {@code kf-spring} so {@code kf-core} keeps no metrics dependency; it is
 * installed into {@code kf-core}'s {@code Observability} holder by
 * {@code KfBootstrap} at startup.
 *
 * <p>Every meter carries a {@code node} tag (the cluster node id, or {@code single}
 * when cluster-disabled) so a Prometheus scrape across nodes can break results
 * down per node without relying on the {@code instance} label. The four latency
 * timers ({@code kf.command.handle}, {@code kf.events.append},
 * {@code kf.segment.tail.read}, {@code kf.sql.execute}) publish a percentile
 * histogram so {@code histogram_quantile(...)} works in PromQL; the per-segment
 * dispatch timers stay plain (count/sum/max) to bound series cardinality.
 */
public class MicrometerTimers implements ObservabilityInterface {

    private final MeterRegistry registry;
    private final String node;
    // Gates the extended bottleneck meters (append-phase split, in-flight /
    // lag gauges, nudge counters) behind kf.observability.extended-metrics so
    // the baseline meter set stays unchanged unless explicitly opted in.
    private final boolean extendedMetrics;
    // Gauges are registered once per tag-set and updated through the held
    // AtomicLong (re-registering a gauge per call would leak meters).
    private final ConcurrentHashMap<String, AtomicLong> gauges = new ConcurrentHashMap<>();

    public MicrometerTimers(MeterRegistry registry, String nodeId) {
        this(registry, nodeId, false);
    }

    public MicrometerTimers(MeterRegistry registry, String nodeId, boolean extendedMetrics) {
        this.registry = registry;
        this.node = (nodeId == null || nodeId.isBlank()) ? "single" : nodeId;
        this.extendedMetrics = extendedMetrics;
    }

    private void record(String name, boolean histogram, long nanos, String... tags) {
        Timer.builder(name)
                .tags(withNode(tags))
                .publishPercentileHistogram(histogram)
                .register(registry)
                .record(nanos, TimeUnit.NANOSECONDS);
    }

    private void count(String name, String... tags) {
        registry.counter(name, withNode(tags)).increment();
    }

    /** Append the common {@code node} tag to a flat {@code k,v,k,v} tag array. */
    private String[] withNode(String[] tags) {
        String[] out = new String[tags.length + 2];
        System.arraycopy(tags, 0, out, 0, tags.length);
        out[tags.length] = "node";
        out[tags.length + 1] = node;
        return out;
    }

    private static String s(String v) {
        return (v == null || v.isBlank()) ? "unknown" : v;
    }

    private static String b(boolean ok) {
        return ok ? "true" : "false";
    }

    @Override
    public void onCommandHandled(String group, String commandType, long nanos, boolean ok) {
        record("kf.command.handle", true, nanos, "group", s(group), "type", s(commandType), "ok", b(ok));
    }

    @Override
    public void onAggregateRehydrated(String aggregateType, int eventsReplayed, long nanos) {
        record("kf.aggregate.rehydrate", true, nanos, "type", s(aggregateType));
        registry.summary("kf.aggregate.rehydrate.events", withNode(new String[]{"type", s(aggregateType)}))
                .record(eventsReplayed);
    }

    @Override
    public void onEventsAppended(int count, long nanos) {
        record("kf.events.append", true, nanos);
        registry.summary("kf.events.append.batch", withNode(new String[0])).record(count);
    }

    @Override
    public void onEventDispatched(String group, int segment, String eventType, long nanos, boolean ok) {
        record("kf.event.dispatch", false, nanos,
                "group", s(group), "segment", Integer.toString(segment), "type", s(eventType), "ok", b(ok));
    }

    @Override
    public void onSagaDispatched(String group, int segment, String eventType, long nanos, boolean ok) {
        record("kf.saga.dispatch", false, nanos,
                "group", s(group), "segment", Integer.toString(segment), "type", s(eventType), "ok", b(ok));
    }

    @Override
    public void onSegmentTailRead(String group, int eventsRead, long nanos) {
        record("kf.segment.tail.read", true, nanos, "group", s(group));
        registry.summary("kf.segment.tail.read.events", withNode(new String[]{"group", s(group)}))
                .record(eventsRead);
    }

    @Override
    public void onCheckpointSaved(String group, int segment) {
        count("kf.checkpoint.save", "group", s(group), "segment", Integer.toString(segment));
    }

    @Override
    public void onDlqEnqueued(String group, String eventType) {
        count("kf.dlq.enqueue", "group", s(group), "type", s(eventType));
    }

    @Override
    public void onSqlExecuted(String category, long nanos, boolean ok) {
        record("kf.sql.execute", true, nanos, "category", s(category), "ok", b(ok));
    }

    /**
     * Look up (or register once) the gauge backing for a {@code (meter, tags)}
     * pair; subsequent calls only touch the AtomicLong.
     */
    private AtomicLong gauge(String name, String... tags) {
        String key = name + "|" + String.join("|", tags);
        return gauges.computeIfAbsent(key, k -> {
            AtomicLong value = new AtomicLong();
            registry.gauge(name, Tags.of(withNode(tags)), value);
            return value;
        });
    }

    @Override
    public void onAppendPhase(String phase, long nanos) {
        if (!extendedMetrics) return;
        // 7 phases x node, histogram on each: bounded. Deliberately no segment
        // tag here so the percentile-histogram series count stays small.
        record("kf.append.phase", true, nanos, "phase", s(phase));
    }

    @Override
    public void onAppendInFlight(int segment, int delta) {
        if (!extendedMetrics) return;
        gauge("kf.append.inflight", "segment", Integer.toString(segment)).addAndGet(delta);
    }

    @Override
    public void onAppendBatch(int requests, int events) {
        if (!extendedMetrics) return;
        registry.summary("kf.append.coalesce.requests", withNode(new String[0])).record(requests);
        registry.summary("kf.append.coalesce.events", withNode(new String[0])).record(events);
    }

    @Override
    public void onPumpNudge(boolean deferred) {
        if (!extendedMetrics) return;
        count("kf.pump.nudge", "deferred", b(deferred));
    }

    @Override
    public void onPumpLag(String group, int segment, long lagEvents) {
        if (!extendedMetrics) return;
        gauge("kf.pump.lag", "group", s(group), "segment", Integer.toString(segment)).set(lagEvents);
    }

    @Override
    public void onCommandForwarded(String commandType, String targetNode, boolean sync, long nanos, boolean ok) {
        // target tag cardinality == cluster size: small by construction.
        record("kf.command.forward", true, nanos,
                "type", s(commandType), "target", s(targetNode), "sync", b(sync), "ok", b(ok));
    }

    @Override
    public void onForwardFallback(String commandType, String reason) {
        count("kf.command.forward.fallback", "type", s(commandType), "reason", s(reason));
    }

    @Override
    public void onForwardServed(String commandType, boolean sync, long nanos, boolean ok) {
        record("kf.command.forward.serve", true, nanos, "type", s(commandType), "sync", b(sync), "ok", b(ok));
    }

    @Override
    public void onRoutingRefreshed(int assignments, int forwardableNodes, long nanos) {
        record("kf.routing.refresh", false, nanos);
        gauge("kf.routing.assignments").set(assignments);
        gauge("kf.routing.forwardable.nodes").set(forwardableNodes);
    }
}
