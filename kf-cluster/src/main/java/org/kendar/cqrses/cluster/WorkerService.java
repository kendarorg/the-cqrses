package org.kendar.cqrses.cluster;

import org.kendar.cqrses.db.Db;
import org.kendar.cqrses.db.DbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Worker loop: claims the partitions the leader assigned to this node, runs each {@link
 * ItemProcessor#process(int)} pump on its own dedicated thread, renews the processing leases, and
 * — on loss of ownership — asks the app to wind a partition down while keeping the lease alive
 * until the app calls {@link #release(int)}.
 * <p>
 * The {@code owner_node} / {@code lease_holder} split is the no-double-pump core: the leader flips
 * {@code owner_node} at handoff, but this node keeps renewing {@code lease_holder = self}, so the
 * gaining node's claim CAS fails for the whole app-paced wind-down. Only after {@code release}
 * clears the lease can the new owner claim.
 * <p>
 * <b>Fail-safe:</b> any DB error aborts the tick as a no-op — in-memory pumps keep running, and the
 * tick is retried next cycle / next poke.
 */
public class WorkerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerService.class.getName());

    private final Db db;
    private final String nodeId;
    private final ClusterClock clock;
    private final ItemProcessor processor;

    /** Partitions for which this node holds a lease (pumping or winding down). */
    private final Set<Integer> active = ConcurrentHashMap.newKeySet();
    /** Partitions told to {@code stopProcess} and awaiting {@code release}. */
    private final Set<Integer> stopping = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<Integer, Thread> pumps = new ConcurrentHashMap<>();
    private final Object lock = new Object();

    private ScheduledExecutorService scheduler;

    public WorkerService(Db db, String nodeId, ClusterClock clock, ItemProcessor processor) {
        this.db = db;
        this.nodeId = nodeId;
        this.clock = clock;
        this.processor = processor;
    }

    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cluster-worker");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::workerTick,
                ClusterConfig.WORKER_TICK, ClusterConfig.WORKER_TICK, TimeUnit.MILLISECONDS);
    }

    /**
     * One reconcile pass. Package-visible test seam; also the body run by the scheduler and by a
     * {@code /notify} poke. Serialized via {@code lock} so the scheduler and a poke cannot race.
     */
    void workerTick() {
        synchronized (lock) {
            long now = clock.now();
            try {
                List<Integer> desired = db.queryForList(
                        "SELECT item_id FROM cluster_assignments WHERE owner_node = ?", Integer.class, nodeId);

                // 1. Claim newly-desired partitions not yet held; start the pump on a win.
                for (Integer item : desired) {
                    if (active.contains(item)) {
                        continue;
                    }
                    int won = db.update("""
                            UPDATE cluster_assignments SET lease_holder = ?, lease_until = ?
                             WHERE item_id = ? AND owner_node = ? AND (lease_until IS NULL OR lease_until < ?)
                            """, nodeId, now + ClusterConfig.LEASE, item, nodeId, now);
                    if (won == 1) {
                        active.add(item);
                        startPump(item);
                    }
                    // A failed claim means the losing node's lease is still live — retry next tick.
                }

                // 2. For partitions we hold but no longer own: ask the app to wind down (once).
                for (Integer item : active) {
                    if (!desired.contains(item) && stopping.add(item)) {
                        try {
                            processor.stopProcess(item);
                        } catch (RuntimeException e) {
                            LOGGER.warn("stopProcess({}) threw: {}", item, e.getMessage());
                        }
                    }
                }

                // 3. Renew every held lease in ONE statement, keyed on lease_holder so it
                //    survives the owner flip. The per-row lease_holder predicate keeps the
                //    fencing identical to N single-row UPDATEs: a row whose lease was cleared
                //    or taken over is simply not matched.
                if (!active.isEmpty()) {
                    List<Integer> held = List.copyOf(active);
                    StringBuilder in = new StringBuilder();
                    Object[] args = new Object[held.size() + 2];
                    args[0] = now + ClusterConfig.LEASE;
                    args[1] = nodeId;
                    for (int i = 0; i < held.size(); i++) {
                        in.append(i == 0 ? "?" : ",?");
                        args[i + 2] = held.get(i);
                    }
                    db.update(
                            "UPDATE cluster_assignments SET lease_until = ? WHERE lease_holder = ? AND item_id IN ("
                                    + in + ")",
                            args);
                }
            } catch (DbException e) {
                // Fail-safe: leave in-memory pumps running, retry next tick.
                LOGGER.warn("worker tick no-op on DB error: {}", e.getMessage());
            }
        }
    }

    /**
     * App-facing callback: the application has finished winding partition {@code itemId} down.
     * Clears the lease (so the gaining node can claim), stops renewing, and drops the partition
     * from the active set.
     */
    public void release(int itemId) {
        synchronized (lock) {
            try {
                db.update(
                        "UPDATE cluster_assignments SET lease_holder = NULL, lease_until = NULL WHERE item_id = ? AND lease_holder = ?",
                        itemId, nodeId);
            } catch (DbException e) {
                LOGGER.warn("release({}) lease-clear failed: {}", itemId, e.getMessage());
            }
            active.remove(itemId);
            stopping.remove(itemId);
            Thread pump = pumps.remove(itemId);
            if (pump != null) {
                pump.interrupt();
            }
        }
    }

    private void startPump(int item) {
        Thread pump = new Thread(() -> {
            try {
                processor.process(item);
            } catch (RuntimeException e) {
                LOGGER.warn("process({}) exited with error: {}", item, e.getMessage());
            }
        }, "cluster-pump-" + item);
        pump.setDaemon(true);
        pumps.put(item, pump);
        pump.start();
    }

    /** Snapshot of partitions currently held — test seam. */
    Set<Integer> activeItems() {
        return Set.copyOf(active);
    }

    public void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        synchronized (lock) {
            for (Thread pump : pumps.values()) {
                pump.interrupt();
            }
            pumps.clear();
            active.clear();
            stopping.clear();
        }
    }
}
