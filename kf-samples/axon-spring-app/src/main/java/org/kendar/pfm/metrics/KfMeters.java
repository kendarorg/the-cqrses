package org.kendar.pfm.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.kendar.pfm.config.PfmProperties;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Port of kf-spring's {@code MicrometerTimers}: the single sink that turns each measured Axon hot
 * path into the SAME {@code kf.*} Micrometer meter (same names, same tags, same {@code node} tag)
 * the kf cluster-IT's Prometheus report queries — so {@code writeAnalysisReport}'s PromQL renders
 * unchanged against the Axon stack.
 *
 * <p>The four latency timers ({@code kf.command.handle}, {@code kf.events.append},
 * {@code kf.segment.tail.read}, {@code kf.sql.execute}) publish a percentile histogram so
 * {@code histogram_quantile(...)} works; the per-segment dispatch timer stays plain to bound series
 * cardinality. The {@code node} tag is the cluster node id (or {@code single} when not clustered),
 * mirroring kf so a cross-node scrape can break results down per node.
 *
 * <p>The Axon hooks that feed this sink live in {@code KfMetricsConfig}. Calls must be cheap and
 * non-throwing — they run on dispatch threads.
 */
@Component
public class KfMeters {

    private final MeterRegistry registry;
    private final String node;

    public KfMeters(MeterRegistry registry, PfmProperties props) {
        this.registry = registry;
        String nodeId = props.getCluster().getNodeId();
        this.node = (nodeId == null || nodeId.isBlank()) ? "single" : nodeId;
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

    /** A command handler ran. */
    public void onCommandHandled(String group, String commandType, long nanos, boolean ok) {
        record("kf.command.handle", true, nanos, "group", s(group), "type", s(commandType), "ok", b(ok));
    }

    /** An aggregate was rehydrated from its event stream. */
    public void onAggregateRehydrated(String aggregateType, int eventsReplayed, long nanos) {
        record("kf.aggregate.rehydrate", true, nanos, "type", s(aggregateType));
        registry.summary("kf.aggregate.rehydrate.events", withNode(new String[]{"type", s(aggregateType)}))
                .record(eventsReplayed);
    }

    /** A batch of emitted events was appended to the event store. */
    public void onEventsAppended(int count, long nanos) {
        record("kf.events.append", true, nanos);
        registry.summary("kf.events.append.batch", withNode(new String[0])).record(count);
    }

    /** A projection event handler ran for one event in {@code segment}. */
    public void onEventDispatched(String group, int segment, String eventType, long nanos, boolean ok) {
        record("kf.event.dispatch", false, nanos,
                "group", s(group), "segment", Integer.toString(segment), "type", s(eventType), "ok", b(ok));
    }

    /** A streaming processor read a tail batch from the event store. */
    public void onSegmentTailRead(String group, int eventsRead, long nanos) {
        record("kf.segment.tail.read", true, nanos, "group", s(group));
        registry.summary("kf.segment.tail.read.events", withNode(new String[]{"group", s(group)}))
                .record(eventsRead);
    }

    /** An event was dead-lettered for {@code group}. */
    public void onDlqEnqueued(String group, String eventType) {
        count("kf.dlq.enqueue", "group", s(group), "type", s(eventType));
    }

    /** One SQL statement executed through the datasource. */
    public void onSqlExecuted(String category, long nanos, boolean ok) {
        record("kf.sql.execute", true, nanos, "category", s(category), "ok", b(ok));
    }
}
