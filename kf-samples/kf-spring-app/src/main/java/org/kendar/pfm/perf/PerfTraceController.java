package org.kendar.pfm.perf;

import org.kendar.cqrses.observability.InMemoryTraceSink;
import org.kendar.cqrses.observability.PerfStage;
import org.kendar.cqrses.observability.PerfTrace;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Harvest surface for the sampled per-command perf traces — the cluster IT pulls
 * each node's in-memory ring over HTTP (deliberately not via the database: perf
 * data persisted to the measured store would perturb the measurement).
 *
 * <pre>
 * GET /kf/perf-traces[?clear=true] -&gt; {node, dropped, traces:[{traceId, commandType,
 *     aggregateId, startedAt, ok, stages:[{stage, nanos, detail}]}]}
 * </pre>
 *
 * Only present when tracing is on — gated on the same property that creates the
 * {@link InMemoryTraceSink} bean (a bean condition would evaluate before the
 * auto-configuration registers the sink).
 */
@RestController
@RequestMapping("/kf")
@ConditionalOnProperty(name = "kf.observability.trace.enabled")
public class PerfTraceController {

    public record StageDto(String stage, long nanos, long detail) {
        static StageDto of(PerfStage s) {
            return new StageDto(s.stage(), s.nanos(), s.detail());
        }
    }

    public record TraceDto(String traceId, String commandType, String aggregateId,
                           long startedAt, boolean ok, List<StageDto> stages) {
        static TraceDto of(PerfTrace t) {
            return new TraceDto(
                    t.traceId() == null ? null : t.traceId().toString(),
                    t.commandType(),
                    t.aggregateId() == null ? null : t.aggregateId().toString(),
                    t.startedAtMillis(), t.ok(),
                    t.stages().stream().map(StageDto::of).toList());
        }
    }

    public record TracesDto(String node, long dropped, List<TraceDto> traces) {
    }

    private final InMemoryTraceSink sink;
    private final String node;

    public PerfTraceController(InMemoryTraceSink sink,
                               org.kendar.cqrses.spring.KfProperties properties) {
        this.sink = sink;
        String nodeId = properties.getCluster().getNodeId();
        this.node = (nodeId == null || nodeId.isBlank()) ? "single" : nodeId;
    }

    @GetMapping("/perf-traces")
    public TracesDto traces(@RequestParam(name = "clear", defaultValue = "false") boolean clear) {
        var snapshot = sink.snapshot();
        long dropped = sink.dropped();
        if (clear) {
            sink.clear();
        }
        return new TracesDto(node, dropped, snapshot.stream().map(TraceDto::of).toList());
    }
}
