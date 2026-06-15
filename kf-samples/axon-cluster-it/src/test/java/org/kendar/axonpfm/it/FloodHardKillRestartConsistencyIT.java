package org.kendar.axonpfm.it;

/**
 * Crash variant: hard-kill node3's container (no graceful processor shutdown, no claim release — its
 * token claims simply expire) and later bring it back as a brand-new container / JVM that rejoins
 * under the same {@code node3} id. Proves the read model still converges to exact consistency under a
 * 100-user flood across a real crash + cold restart.
 *
 * <p>The base quiesces the flood and removes node3 from write rotation before {@link #takeDownNode3()},
 * so the crash cannot orphan an ambiguous in-flight write.
 */
class FloodHardKillRestartConsistencyIT extends AbstractFloodConsistencyIT {

    @Override
    protected void takeDownNode3() {
        // No /cluster/stop — just kill the container. Claims expire via the token claim timeout.
        stopNodeContainer(NODE3);
    }

    @Override
    protected void bringBackNode3() {
        // Recreate from the same config (same node id, alias, pinned ports). Blocks until the new
        // node's wait strategy passes (/cluster/status answers 200).
        startNodeContainer(NODE3);
    }

    @Override
    protected String variant() {
        return "hard-kill";
    }
}
