package org.kendar.pfm.it;

/**
 * Crash variant of {@link AbstractFloodConsistencyIT}: instead of a graceful API stop, <b>hard-kill
 * node3's container</b> (no leader-lock release, no graceful pump drain — its heartbeat simply goes
 * stale) and later bring it back as a <b>brand-new container / JVM</b> that rejoins under the same
 * {@code node3} id. Proves the read model still converges to exact consistency under a 100-user flood
 * across a real crash + cold restart, not just a graceful leave.
 *
 * <p>The base quiesces the flood and removes node3 from write rotation <i>before</i> calling
 * {@link #takeDownNode3()}, so the crash cannot orphan an ambiguous in-flight write (see the base
 * class javadoc). Slower than the graceful variant: a cold container restart waits out the full image
 * boot + cluster-join handshake.
 */
class FloodHardKillRestartConsistencyIT extends AbstractFloodConsistencyIT {

    @Override
    protected void takeDownNode3() {
        // No /cluster/stop — just kill the container. The JVM dies; the leader lock and heartbeat are
        // never released, so the survivors only notice via staleness + lease expiry.
        stopNodeContainer(NODE3);
    }

    @Override
    protected void bringBackNode3() {
        // Recreate the container from the same config (same node id, network alias, pinned ports). The
        // call blocks until the new node's wait strategy passes (/cluster/status + /alive answer 200),
        // i.e. it has booted and its control surface is live again.
        startNodeContainer(NODE3);
    }

    @Override
    protected String variant() {
        return "hard-kill";
    }
}
