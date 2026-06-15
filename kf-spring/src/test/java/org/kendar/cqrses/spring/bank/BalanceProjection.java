package org.kendar.cqrses.spring.bank;

import org.kendar.cqrses.annotations.EventHandler;
import org.kendar.cqrses.annotations.Projection;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registered as a live Spring bean: the scanner finds the {@code @Projection} and a bean of this
 * type exists, so it is bridged via {@code GlobalRegistry.register(class, bean)}. The
 * {@code on(Deposited, AuditService)} handler takes a collaborator as a non-first parameter, which
 * the bus resolves from {@code GlobalRegistry} (pre-warmed from Spring).
 */
@Projection(group = "balances")
public class BalanceProjection {

    private final Map<UUID, Long> balances = new ConcurrentHashMap<>();

    @EventHandler
    public void on(AccountOpened e) {
        balances.putIfAbsent(e.accountId, 0L);
    }

    @EventHandler
    public void on(Deposited e, AuditService audit) {
        balances.merge(e.accountId, e.amount, Long::sum);
        audit.record(e.accountId);
    }

    public long balanceOf(UUID accountId) {
        return balances.getOrDefault(accountId, 0L);
    }
}
