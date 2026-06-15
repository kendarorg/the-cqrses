package org.kendar.cqrses.integration.bank;

import org.kendar.cqrses.annotations.EventHandler;
import org.kendar.cqrses.annotations.Projection;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Projection(group = "balances")
public class BalanceProjection {
    private final Map<UUID, Long> balances = new ConcurrentHashMap<>();

    @EventHandler
    public void on(AccountOpened e) {
        balances.putIfAbsent(e.accountId, 0L);
    }

    @EventHandler
    public void on(Deposited e) {
        balances.merge(e.accountId, e.amount, Long::sum);
    }

    @EventHandler
    public void on(Withdrawn e) {
        balances.merge(e.accountId, -e.amount, Long::sum);
    }

    public long balanceOf(UUID accountId) {
        return balances.getOrDefault(accountId, 0L);
    }

    public Map<UUID, Long> snapshot() {
        return new HashMap<>(balances);
    }
}
