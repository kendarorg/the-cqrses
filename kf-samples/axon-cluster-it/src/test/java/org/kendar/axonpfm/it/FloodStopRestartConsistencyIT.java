package org.kendar.axonpfm.it;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Graceful variant: node3 leaves and rejoins through the app's {@code /cluster/*} control API, so its
 * JVM, ports and HTTP surface stay up the whole time — only its streaming processors stop (releasing
 * token claims). Proves the read model stays exactly consistent across a graceful stop/restart under a
 * 100-user flood.
 */
class FloodStopRestartConsistencyIT extends AbstractFloodConsistencyIT {

    @Override
    protected void takeDownNode3() {
        String stopped = clusterStop(NODE3);
        assertThat(jsonBool(stopped, "enabled")).isTrue();
        assertThat(jsonBool(stopped, "running")).isFalse();
        assertThat(clusterRunning(NODE3)).isFalse();
    }

    @Override
    protected void bringBackNode3() {
        String started = clusterStart(NODE3);
        assertThat(jsonBool(started, "running")).isTrue();
    }

    @Override
    protected String variant() {
        return "api-stop";
    }
}
