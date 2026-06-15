package org.kendar.cqrses.integration.jdbc;

import org.kendar.cqrses.integration.AbstractBankDlqTest;
import org.kendar.cqrses.integration.IntegrationBackend;

/** Runs {@link AbstractBankDlqTest} against the {@code kf-core-db} JDBC stores and buses. */
class JdbcBankDlqTest extends AbstractBankDlqTest {
    @Override
    protected IntegrationBackend createBackend() {
        return new JdbcBackend();
    }
}
