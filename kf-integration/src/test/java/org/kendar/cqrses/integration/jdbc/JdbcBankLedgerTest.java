package org.kendar.cqrses.integration.jdbc;

import org.kendar.cqrses.integration.AbstractBankLedgerTest;
import org.kendar.cqrses.integration.IntegrationBackend;

/** Runs {@link AbstractBankLedgerTest} against the {@code kf-core-db} JDBC stores and buses. */
class JdbcBankLedgerTest extends AbstractBankLedgerTest {
    @Override
    protected IntegrationBackend createBackend() {
        return new JdbcBackend();
    }
}
