package org.kendar.cqrses.integration.memory;

import org.kendar.cqrses.integration.AbstractBankDlqTest;
import org.kendar.cqrses.integration.IntegrationBackend;

/** Runs {@link AbstractBankDlqTest} against the {@code kf-core-memory} heap stores and buses. */
class InMemoryBankDlqTest extends AbstractBankDlqTest {
    @Override
    protected IntegrationBackend createBackend() {
        return new InMemoryBackend();
    }
}
