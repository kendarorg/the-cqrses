package org.kendar.pfm.dlq;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * DLQ-control REST surface — the SAME contract as the kf sample.
 *
 * <pre>
 * POST /dlq/retry-all -&gt; 200 {attempted, resolved, failed}
 * </pre>
 *
 * Re-runs every pending dead-lettered projection event at least once (idempotent). Used by the
 * cluster consistency IT to drain stranded projection events before its final read-model verdict.
 */
@RestController
@RequestMapping("/dlq")
public class DlqController {

    private final DlqControl control;

    public DlqController(DlqControl control) {
        this.control = control;
    }

    @PostMapping("/retry-all")
    public DlqControl.RetryReport retryAll() {
        return control.retryAll();
    }
}
