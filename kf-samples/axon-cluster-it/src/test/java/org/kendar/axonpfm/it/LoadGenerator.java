package org.kendar.axonpfm.it;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A background flood of {@code RecordOperation}s across the cluster, with deterministic bookkeeping of
 * what the read model <i>should</i> converge to. Framework-neutral (speaks only HTTP) — a verbatim
 * copy of the kf cluster-IT's generator so the two stacks are loaded identically.
 *
 * <p>Worker threads pick a random user, the next <b>currently-live</b> node in round-robin order, and
 * a random {@code IN}/{@code OUT} amount, and POST it through the supplied {@link OpDriver}. A throwing
 * call is <b>not</b> counted, so the running totals only reflect acked ops. {@link #pauseAndQuiesce()}
 * takes a write lock so totals are stable for a consistency checkpoint; {@link #resume()} continues.
 */
final class LoadGenerator {

    @FunctionalInterface
    interface OpDriver {
        String record(int nodeIdx, String user, String type, long amount, String tag);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger("axon-cluster-it");
    private static final String[] TAGS = {"salary", "rent", "food", "bonus", "coffee", "travel"};
    private static final int IN_PERCENT = 70;
    private static final int MAX_AMOUNT = 100;

    private final OpDriver driver;
    private final List<String> users;
    private final int threads;
    private final long throttleMs;

    final Set<Integer> liveNodes = ConcurrentHashMap.newKeySet();

    final Map<String, AtomicLong> expectedNet = new ConcurrentHashMap<>();
    private final AtomicLong grandNet = new AtomicLong();
    private final AtomicLong ackedOps = new AtomicLong();

    private final AtomicLong nodeCursor = new AtomicLong();

    final Set<String> ackedOpIds = ConcurrentHashMap.newKeySet();

    private final AtomicLong failedOps = new AtomicLong();
    private final Map<String, AtomicLong> failureSignatures = new ConcurrentHashMap<>();

    private final ReentrantReadWriteLock gate = new ReentrantReadWriteLock();

    private volatile boolean running;
    private ExecutorService pool;

    LoadGenerator(OpDriver driver, List<String> users, int threads, long throttleMs) {
        this.driver = driver;
        this.users = List.copyOf(users);
        this.threads = threads;
        this.throttleMs = throttleMs;
        for (String u : this.users) {
            expectedNet.put(u, new AtomicLong());
        }
    }

    void start() {
        running = true;
        pool = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "flood-worker");
            t.setDaemon(true);
            return t;
        });
        for (int i = 0; i < threads; i++) {
            pool.submit(this::runWorker);
        }
        LOGGER.trace("load generator started: {} threads, {} users, live nodes {}",
                threads, users.size(), liveNodes);
    }

    private void runWorker() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        while (running) {
            gate.readLock().lock();
            try {
                if (!running) {
                    return;
                }
                Integer[] live = liveNodes.toArray(new Integer[0]);
                if (live.length > 0) {
                    doOneOp(rnd, live);
                }
            } catch (RuntimeException e) {
                recordFailure(e);
            } finally {
                gate.readLock().unlock();
            }
            sleep(throttleMs);
        }
    }

    private void doOneOp(ThreadLocalRandom rnd, Integer[] live) {
        int node = live[(int) (Math.floorMod(nodeCursor.getAndIncrement(), live.length))];
        String user = users.get(rnd.nextInt(users.size()));
        boolean in = rnd.nextInt(100) < IN_PERCENT;
        long amount = 1 + rnd.nextInt(MAX_AMOUNT);
        String tag = TAGS[rnd.nextInt(TAGS.length)];

        String opId = driver.record(node, user, in ? "IN" : "OUT", amount, tag);

        long delta = in ? amount : -amount;
        expectedNet.get(user).addAndGet(delta);
        grandNet.addAndGet(delta);
        ackedOps.incrementAndGet();
        if (opId != null) {
            ackedOpIds.add(opId);
        }
    }

    private void recordFailure(RuntimeException e) {
        failedOps.incrementAndGet();
        Throwable root = e;
        while (root.getCause() != null) root = root.getCause();
        String sig = e.getClass().getSimpleName() + " / root=" + root.getClass().getSimpleName();
        if (e.getMessage() != null && e.getMessage().contains("→ 5")) sig += " (5xx)";
        failureSignatures.computeIfAbsent(sig, k -> new AtomicLong()).incrementAndGet();
    }

    void pauseAndQuiesce() {
        gate.writeLock().lock();
        LOGGER.trace("load generator quiesced at {} acked ops (net {})", ackedOps.get(), grandNet.get());
    }

    void resume() {
        gate.writeLock().unlock();
        LOGGER.trace("load generator resumed");
    }

    void stop() {
        running = false;
        if (pool != null) {
            pool.shutdown();
            try {
                if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
                    LOGGER.warn("load generator workers did not terminate within 30s; forcing shutdown");
                    pool.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        LOGGER.trace("load generator stopped at {} acked ops (net {})", ackedOps.get(), grandNet.get());
        LOGGER.trace("load generator failure summary: {} failed ops; signatures={}",
                failedOps.get(), failureSignatures);
    }

    long ackedOps() {
        return ackedOps.get();
    }

    Set<String> ackedOpIds() {
        return ackedOpIds;
    }

    long failedOps() {
        return failedOps.get();
    }

    long grandNet() {
        return grandNet.get();
    }

    long expectedNet(String user) {
        return expectedNet.get(user).get();
    }

    private static void sleep(long ms) {
        if (ms <= 0) {
            return;
        }
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
