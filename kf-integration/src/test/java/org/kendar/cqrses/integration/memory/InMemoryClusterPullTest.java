package org.kendar.cqrses.integration.memory;

import org.kendar.cqrses.integration.AbstractClusterPullTest;
import org.kendar.cqrses.integration.IntegrationBackend;

/** In-memory binding of the cluster pull-pump suite. */
class InMemoryClusterPullTest extends AbstractClusterPullTest {
    @Override
    protected IntegrationBackend createBackend() {
        return new InMemoryBackend();
    }
}
