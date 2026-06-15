package org.kendar.cqrses.pg;

import org.kendar.cqrses.cluster.spi.SegmentOwnership;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Single-node driver for the {@link SegmentOwnership} pull engine: it claims
 * <b>every</b> segment {@code 0..N-1} locally, so a cluster-free JDBC deployment
 * still runs the {@code SegmentProcessor} (durable, checkpointed read models)
 * instead of push lanes — the degenerate "cluster of one owns everything" case.
 * <p>
 * The cluster and the single-node case share the same {@code SegmentProcessor};
 * they differ only in the <i>owner</i>: the cluster's {@code WorkerService} hands
 * out segment <i>subsets</i> through the {@code ItemProcessor} bridge, while this
 * owner hands out all of them. It depends on nothing but the {@code kf-core}
 * {@link SegmentOwnership} SPI — <b>no {@code kf-cluster} dependency</b> is dragged
 * into single-node JDBC.
 * <p>
 * {@link SegmentOwnership#claimSegment} blocks for the lifetime of ownership, so
 * each segment is claimed on its own daemon thread; {@link #stop()} releases each
 * (draining in-flight dispatch and the last checkpoint) and joins the threads.
 */
public class LocalSegmentOwner {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalSegmentOwner.class);
    private static final long DRAIN_TIMEOUT_MS = 15_000L;
    private static final long JOIN_TIMEOUT_MS = 5_000L;

    private final SegmentOwnership ownership;
    private final int segments;
    private final List<Thread> claimers = new ArrayList<>();
    private boolean started;

    public LocalSegmentOwner(SegmentOwnership ownership, int segments) {
        this.ownership = ownership;
        this.segments = segments;
    }

    /** Claim every segment {@code 0..segments-1}, each on its own parked daemon thread. */
    public synchronized void start() {
        if (started) return;
        started = true;
        for (int i = 0; i < segments; i++) {
            final int seg = i;
            Thread t = new Thread(() -> ownership.claimSegment(seg), "local-seg-owner-" + seg);
            t.setDaemon(true);
            claimers.add(t);
            t.start();
        }
        LOGGER.info("LocalSegmentOwner claimed all {} segment(s)", segments);
    }

    /**
     * Release every owned segment (draining in-flight dispatch + the final
     * checkpoint), then join the claim threads. Idempotent.
     */
    public synchronized void stop() {
        if (!started) return;
        started = false;
        CountDownLatch drained = new CountDownLatch(segments);
        for (int i = 0; i < segments; i++) {
            ownership.releaseSegment(i, drained::countDown);
        }
        try {
            if (!drained.await(DRAIN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                LOGGER.warn("LocalSegmentOwner: not all segments drained within {} ms", DRAIN_TIMEOUT_MS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        for (Thread t : claimers) {
            try {
                t.join(JOIN_TIMEOUT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        claimers.clear();
        LOGGER.info("LocalSegmentOwner released all {} segment(s)", segments);
    }
}
