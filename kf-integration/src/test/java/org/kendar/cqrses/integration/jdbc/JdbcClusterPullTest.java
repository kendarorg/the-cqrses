package org.kendar.cqrses.integration.jdbc;

import org.kendar.cqrses.integration.AbstractClusterPullTest;
import org.kendar.cqrses.integration.IntegrationBackend;

/** JDBC (H2 MODE=MySQL) binding of the cluster pull-pump suite. */
class JdbcClusterPullTest extends AbstractClusterPullTest {
    @Override
    protected IntegrationBackend createBackend() {
        return new JdbcBackend();
    }
}
