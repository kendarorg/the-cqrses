package org.kendar.cqrses.integration.bank;

import org.kendar.cqrses.annotations.EventHandler;
import org.kendar.cqrses.annotations.Projection;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A compliance-side projection in its own processing group ("fraud") that clears
 * deposits for anti-fraud review. An account under a fraud hold cannot have its
 * deposits cleared: the handler throws, so the {@code Deposited} event is routed
 * to the DLQ by the group's {@link org.kendar.cqrses.dlq.ErrorPolicy#DLQ_SKIP}
 * policy while every other group (e.g. "balances") keeps flowing.
 *
 * <p>The failure is transient and operator-fixable: once the hold is lifted via
 * {@link #release(UUID)}, replaying the dead letter clears the deposit. This is
 * the canonical "poison message → dead letter → fix root cause → retry → resolve"
 * lifecycle, expressed in the bank domain.
 */
@Projection(group = "fraud")
public class FraudMonitor {

    private final Set<UUID> heldAccounts = ConcurrentHashMap.newKeySet();
    private final Map<UUID, Long> cleared = new ConcurrentHashMap<>();

    /** Place an account under a fraud hold: deposits for it will dead-letter. */
    public void hold(UUID accountId) {
        heldAccounts.add(accountId);
    }

    /** Lift the fraud hold so a retried (or fresh) deposit can be cleared. */
    public void release(UUID accountId) {
        heldAccounts.remove(accountId);
    }

    @EventHandler
    public void on(Deposited e) {
        if (heldAccounts.contains(e.accountId)) {
            throw new IllegalStateException(
                    "Account " + e.accountId + " is under a fraud hold; deposit of "
                            + e.amount + " cannot be cleared");
        }
        cleared.merge(e.accountId, e.amount, Long::sum);
    }

    /** Total amount cleared for an account by anti-fraud review. */
    public long clearedTotal(UUID accountId) {
        return cleared.getOrDefault(accountId, 0L);
    }
}
