package org.kendar.cqrses.spring.observability;

import org.kendar.cqrses.db.Db;
import org.kendar.cqrses.observability.Observability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.util.HashMap;
import java.util.Map;

/**
 * Periodically samples the pull-pump backlog — head of each {@code segment_counter}
 * minus the committed {@code processor_checkpoint} per {@code (group, segment)} —
 * and publishes it through {@code Observability.onPumpLag}. Two small SELECTs per
 * interval; never throws (a sampler must not take the node down).
 *
 * <p>Part of the extended-metrics set ({@code kf.observability.extended-metrics});
 * the interval comes from {@code kf.observability.lag.sample-interval-ms}
 * ({@code 0} = off). A {@link SmartLifecycle} one phase after {@code KfBootstrap}:
 * it starts once the framework is up and stops before it tears down.
 */
public class KfPumpLagSampler implements SmartLifecycle {

    private static final Logger LOGGER = LoggerFactory.getLogger(KfPumpLagSampler.class);

    private record GroupSegment(String group, int segment) {
    }

    private final Db db;
    private final long intervalMs;
    private volatile boolean running;
    private Thread thread;

    public KfPumpLagSampler(Db db, long intervalMs) {
        this.db = db;
        this.intervalMs = intervalMs;
    }

    @Override
    public synchronized void start() {
        if (running || intervalMs <= 0) return;
        running = true;
        thread = new Thread(this::loop, "kf-pump-lag-sampler");
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public synchronized void stop() {
        running = false;
        if (thread != null) {
            thread.interrupt();
            thread = null;
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        // One phase later than KfBootstrap (MAX-1000): start after the framework
        // is fully up, stop before it tears down.
        return Integer.MAX_VALUE - 999;
    }

    private void loop() {
        while (running) {
            try {
                sampleOnce();
            } catch (Exception e) {
                // log-throttled enough at this cadence; never propagate
                LOGGER.debug("pump-lag sample failed: {}", e.getMessage());
            }
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    void sampleOnce() {
        // head per segment: next_seq is the NEXT seq to assign, so head-1 is the last appended.
        Map<Integer, Long> heads = new HashMap<>();
        db.query("SELECT segment, next_seq FROM segment_counter",
                (rs, n) -> heads.put(rs.getInt(1), rs.getLong(2)));
        if (heads.isEmpty()) return;
        // committed watermark per (group, segment); projection groups have segment == source_segment.
        Map<GroupSegment, Long> marks = new HashMap<>();
        db.query("SELECT processing_group, segment, last_seq FROM processor_checkpoint "
                        + "WHERE segment = source_segment",
                (rs, n) -> marks.put(new GroupSegment(rs.getString(1), rs.getInt(2)), rs.getLong(3)));
        var obs = Observability.get();
        for (var e : marks.entrySet()) {
            Long head = heads.get(e.getKey().segment());
            if (head == null) continue;
            long lag = Math.max(0, (head - 1) - e.getValue());
            obs.onPumpLag(e.getKey().group(), e.getKey().segment(), lag);
        }
    }
}
