package org.kendar.pfm.dlq;

import org.kendar.cqrses.bus.EventBus;
import org.kendar.cqrses.dlq.DlqManager;
import org.kendar.cqrses.dlq.DlqStore;
import org.kendar.cqrses.dlq.LocalDlqManager;
import org.kendar.pfm.read.Uuids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

/**
 * Operator/test control to drain the projection DLQ: re-run every dead-lettered event at least once
 * through the live (event-side) handler topology. Re-running is safe and repeatable — the read model
 * is insert-ignore on {@code op_id}, so a re-applied event can never double-count.
 *
 * <p>The shared {@code dlq_item} table is enumerated directly (the {@link DlqStore} SPI keys lookups
 * by {@code sequenceId}, so there is no "list every pending item" method); each PENDING item is then
 * retried through a node-local {@link LocalDlqManager}, which re-invokes the handler in an isolated
 * throwaway group and, on success, resolves the item and clears its per-aggregate head-of-line block.
 * Any single node can drain the whole queue — all nodes carry the same projection handlers and write
 * to the same read model.
 */
@Component
public class DlqControl {

    private static final Logger LOGGER = LoggerFactory.getLogger(DlqControl.class);

    private final EventBus eventBus;
    private final DlqStore dlqStore;
    private final JdbcTemplate jdbc;

    /** Built lazily on first use: its constructor reads the (by-then-bootstrapped) GlobalRegistry. */
    private volatile DlqManager manager;

    public DlqControl(EventBus eventBus, DlqStore dlqStore, JdbcTemplate jdbc) {
        this.eventBus = eventBus;
        this.dlqStore = dlqStore;
        this.jdbc = jdbc;
    }

    /**
     * Re-run every PENDING dead letter once, in per-aggregate FIFO order ({@code sequence_id},
     * {@code ordinal}). A still-failing item is left in place and counted; it does not abort the
     * sweep, so every item gets at least one attempt.
     */
    public RetryReport retryAll() {
        DlqManager mgr = manager();
        List<UUID> ids = jdbc.query(
                "SELECT id FROM dlq_item WHERE status = 'PENDING' ORDER BY sequence_id, ordinal",
                (rs, i) -> Uuids.fromBytes(rs.getBytes("id")));
        int resolved = 0;
        int failed = 0;
        for (UUID id : ids) {
            try {
                mgr.retry(id);
                resolved++;
            } catch (RuntimeException ex) {
                failed++;
                LOGGER.warn("dlq retry failed for {}: {}", id, ex.getMessage());
            }
        }
        RetryReport report = new RetryReport(ids.size(), resolved, failed);
        if(ids.size()>0) {
            LOGGER.trace("dlq retry-all: {}", report);
        }
        return report;
    }

    private DlqManager manager() {
        DlqManager m = manager;
        if (m == null) {
            synchronized (this) {
                if (manager == null) {
                    manager = new LocalDlqManager(eventBus, dlqStore, eventBus.getHandler());
                }
                m = manager;
            }
        }
        return m;
    }

    /** Outcome of a {@link #retryAll()} sweep. Serialised straight to JSON by the controller. */
    public record RetryReport(int attempted, int resolved, int failed) {
    }
}
