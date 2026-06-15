package org.kendar.pfm.cluster;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Cluster-control REST surface. Always present; every endpoint is a no-op reporting
 * {@code enabled=false} when {@code kf.cluster.enabled=false} (see {@link ClusterControl}).
 *
 * <pre>
 * GET  /cluster/status  -&gt; 200 {enabled, running, nodeId, segments}
 * POST /cluster/stop    -&gt; 200 {enabled, running:false}
 * POST /cluster/start   -&gt; 200 {enabled, running:true}
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
}
