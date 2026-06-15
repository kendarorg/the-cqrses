package org.kendar.pfm.cluster;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Cluster-control REST surface — the SAME contract as the kf sample so the cluster IT drives both
 * stacks identically. Every endpoint reports {@code enabled=false} (and is a no-op) when
 * {@code pfm.cluster.mode=false}.
 *
 * <pre>
 * GET  /cluster/status   -&gt; 200 {enabled, running, nodeId, segments}
 * POST /cluster/stop     -&gt; 200 {enabled, running:false}
 * POST /cluster/start    -&gt; 200 {enabled, running:true}
 * GET  /cluster/segments -&gt; 200 [int,...]   // segments this node currently owns (Axon-only helper)
 * </pre>
 */
@RestController
@RequestMapping("/cluster")
public class ClusterController {

    private final ClusterControl control;

    public ClusterController(ClusterControl control) {
        this.control = control;
    }

    @GetMapping("/status")
    public ClusterControl.Status status() {
        return control.status();
    }

    @PostMapping("/stop")
    public ClusterControl.Status stop() {
        return control.stop();
    }

    @PostMapping("/start")
    public ClusterControl.Status start() {
        return control.start();
    }

    /** Segments this node currently owns — the IT aggregates this across nodes into the owner map. */
    @GetMapping("/segments")
    public List<Integer> segments() {
        return List.copyOf(control.ownedSegments());
    }
}
