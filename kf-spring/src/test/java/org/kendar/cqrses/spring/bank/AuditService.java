package org.kendar.cqrses.spring.bank;

import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Collaborator injected into {@link BalanceProjection}'s event handler as a non-first method
 * parameter. Registered as a {@code @Lazy} Spring bean so that — absent pre-warm — it would be
 * constructed on a dispatch (lane) thread at first use. Pre-warm forces its construction during
 * {@code KfBootstrap.start()} instead; the static {@link #CONSTRUCTIONS} counter lets the test prove
 * it was built before any command was dispatched.
 */
public class AuditService {

    public static final AtomicInteger CONSTRUCTIONS = new AtomicInteger();

    private final ConcurrentLinkedQueue<UUID> recorded = new ConcurrentLinkedQueue<>();

    public AuditService() {
        CONSTRUCTIONS.incrementAndGet();
    }

    public void record(UUID accountId) {
        recorded.add(accountId);
    }

    public int recordedCount() {
        return recorded.size();
    }

    public static void resetConstructions() {
        CONSTRUCTIONS.set(0);
    }
}
