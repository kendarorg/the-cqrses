package org.kendar.cqrses.cluster;

/**
 * Application SPI for partition processing. The work items kf-cluster distributes are the
 * integers {@code 0..N-1}, treated as <b>partition / shard indices</b>, not discrete jobs.
 * <p>
 * kf-cluster's sole guarantee is that <b>exactly one live node runs {@link #process(int)}
 * for a given partition at a time</b>. <i>How</i> to find and process the jobs belonging to
 * partition {@code i} is entirely the application's concern.
 *
 * @see ClusterNode#release(int) the app-facing callback that completes a wind-down
 */
public interface ItemProcessor {

    /**
     * Long-lived pump for partition {@code itemId}. kf-cluster runs this on its own dedicated
     * thread once the node claims the partition. <b>It must never return</b> under normal
     * operation — it keeps pumping the partition's work until asked to stop.
     */
    void process(int itemId);

    /**
     * kf-cluster asks the application to wind partition {@code itemId} down (the node has lost
     * ownership). This is cooperative: signal the pump to stop, and once it has truly stopped,
     * the application calls {@link ClusterNode#release(int)} to release the lease. Until then,
     * kf-cluster keeps the lease alive so the gaining node cannot start a second pump.
     */
    void stopProcess(int itemId);
}
