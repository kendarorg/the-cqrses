package org.kendar.cqrses.integration.memory;

import org.kendar.cqrses.integration.AbstractBankLedgerTest;
import org.kendar.cqrses.integration.IntegrationBackend;

/** Runs {@link AbstractBankLedgerTest} against the {@code kf-core-memory} heap stores and buses. */
class InMemoryBankLedgerTest extends AbstractBankLedgerTest {
    @Override
    protected IntegrationBackend createBackend() {
        return new InMemoryBackend();
    }
}
