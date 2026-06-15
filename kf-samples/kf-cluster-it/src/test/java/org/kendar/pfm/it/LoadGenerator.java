package org.kendar.pfm.it;

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
 * what the read model <i>should</i> converge to.
 *
 * <p>Several worker threads pick a random user, the next <b>currently-live</b> node in <b>round-robin</b>
 * order (a shared cursor cycles through the live set so the flood is spread evenly across all cluster
 * nodes rather than clumping on whichever node a per-thread RNG happens to favour), and a random
 * {@code IN}/{@code OUT} amount, and POST it through the supplied {@link OpDriver}. The driver throws on
 * a non-2xx response (the harness {@code recordOp} contract); a throwing call is <b>not</b> counted, so
 * the running totals only ever reflect operations the server actually acknowledged. Because the PFM
 * read model is insert-ignore on a fresh server-side {@code opId} and the domain applies no balance
 * guard, {@code net = Σ(IN) − Σ(OUT)} over the acked set is exact — the cluster's job is merely to make
 * the read model catch up to it.
 *
 * <h2>Quiescing for consistency checkpoints</h2>
 * The read model is eventually consistent, so you can only assert exact totals when no write is in
 * flight. {@link #pauseAndQuiesce()} takes a write lock that (a) waits for every in-flight op to finish
 * and (b) blocks workers from starting new ones — after it returns, {@link #ackedOps()} /
 * {@link #grandNet()} / {@link #expectedNet} are stable and the read model can be awaited up to them.
 * {@link #resume()} lets the flood continue. Workers park on the lock (they do not busy-spin) while
 * paused. {@code pauseAndQuiesce} / {@code resume} must be called from the same (test) thread.
 *
 * <p>The live-node set is mutable at runtime ({@link #liveNodes}) so a scenario can pull a node out of
 * write rotation before stopping it and add it back after it rejoins. Not reusable after {@link #stop()}.
 */
final class LoadGenerator {

    /**
     * What the generator calls to push one operation; mirrors {@code AbstractClusterIT.recordOp}.
     * Returns the server-minted {@code opId} of the acked op (only reached on a 2xx); throws otherwise.
     */
    @FunctionalInterface
    interface OpDriver {
        String record(int nodeIdx, String user, String type, long amount, String tag);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger("kf-cluster-it");
    private static final String[] TAGS = {"salary", "rent", "food", "bonus", "coffee", "travel"};
    private static final int IN_PERCENT = 70; // 70% credits, 30% debits — net still tracked exactly
    private static final int MAX_AMOUNT = 100;

    private final OpDriver driver;
    private final List<String> users;
    private final int threads;
    private final long throttleMs;

    /** Node indexes the workers may write to right now. Mutable; concurrent. */
    final Set<Integer> liveNodes = ConcurrentHashMap.newKeySet();

    /** Per-user expected {@code net} over all acked ops. */
    final Map<String, AtomicLong> expectedNet = new ConcurrentHashMap<>();
    private final AtomicLong grandNet = new AtomicLong();
    private final AtomicLong ackedOps = new AtomicLong();

    /** Shared cursor that round-robins ops across the live nodes (instead of picking one at random). */
    private final AtomicLong nodeCursor = new AtomicLong();

    /**
     * DIAGNOSTIC: the server-minted opId of every acked op. The durable {@code pfm_operation} rows
     * whose op_id is NOT in here are the committed-but-ack-lost ops (a write that landed durably yet
     * whose 2xx the client never saw). The flood-end diagnostics dump exactly that inverse set.
     */
    final Set<String> ackedOpIds = ConcurrentHashMap.newKeySet();

    /** DIAGNOSTIC: count of throwing ops, and a small sample of distinct failure signatures. */
    private final AtomicLong failedOps = new AtomicLong();
    private final Map<String, AtomicLong> failureSignatures = new ConcurrentHashMap<>();

    /** Read lock = "a worker is doing an op"; write lock = "quiesced for a checkpoint". */
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
                // Transient: a node leaving rotation, an HTTP hiccup, a 4xx/5xx. The op was NOT
                // acked, so it was NOT counted above — just move on. Don't kill the worker.
                // DIAGNOSTIC: a failure here may still have committed durably (commit/response lost
                // after the server persisted) — that is precisely the drift we are hunting. Record
                // its signature so the flood-end dump shows how the ack was lost (5xx vs IOException).
                recordFailure(e);
            } finally {
                gate.readLock().unlock();
            }
            sleep(throttleMs);
        }
    }

    private void doOneOp(ThreadLocalRandom rnd, Integer[] live) {
        // Round-robin across the live nodes: a shared monotonic cursor modulo the live count spreads
        // ops evenly over every cluster node, so each survivor gets a fair share of the flood.
        int node = live[(int) (Math.floorMod(nodeCursor.getAndIncrement(), live.length))];
        String user = users.get(rnd.nextInt(users.size()));
        boolean in = rnd.nextInt(100) < IN_PERCENT;
        long amount = 1 + rnd.nextInt(MAX_AMOUNT);
        String tag = TAGS[rnd.nextInt(TAGS.length)];

        // Throws on non-2xx — on failure we never reach the bookkeeping below, so totals stay exact.
        String opId = driver.record(node, user, in ? "IN" : "OUT", amount, tag);

        long delta = in ? amount : -amount;
        expectedNet.get(user).addAndGet(delta);
        grandNet.addAndGet(delta);
        ackedOps.incrementAndGet();
        if (opId != null) {
            ackedOpIds.add(opId); // DIAGNOSTIC: durable rows not in this set are the ack-lost ops.
        }
    }

    /** DIAGNOSTIC: bucket a failed op by a coarse signature so the dump shows which layer lost the ack. */
    private void recordFailure(RuntimeException e) {
        failedOps.incrementAndGet();
        // Walk to the root cause; "→ 500" (IllegalStateException from a 5xx body) means the server
        // threw after possibly committing (DB commit in-doubt); an IOException cause means the HTTP
        // response itself was lost after a clean server-side commit. Either way: committed-but-unacked.
        Throwable root = e;
        while (root.getCause() != null) root = root.getCause();
        String sig = e.getClass().getSimpleName() + " / root=" + root.getClass().getSimpleName();
        if (e.getMessage() != null && e.getMessage().contains("→ 5")) sig += " (5xx)";
        failureSignatures.computeIfAbsent(sig, k -> new AtomicLong()).incrementAndGet();
    }

    /** Block new ops and wait for in-flight ones to drain; totals are stable until {@link #resume()}. */
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
            // Graceful: do NOT interrupt in-flight POSTs. A worker mid-request may have already
            // reached the server and committed the event durably; interrupting it (shutdownNow)
            // makes HttpClient.send throw InterruptedException, so the op is never acked — yet it
            // lands in the read model, leaving acked below durable (the off-by-≈FLOOD_THREADS drift
            // the final consistency check saw). shutdown() lets each worker finish its current op and
            // ack it before exiting (running=false is observed at the next loop top), so acked ==
            // durable. This mirrors pauseAndQuiesce(), which the mid-flood checkpoints use — and is
            // why those converge exactly while only the final stop() drifted.
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
        // DIAGNOSTIC: how many ops threw, and the distinct failure signatures. A non-zero count here
        // with a matching durable-but-unacked set at the end is the committed-but-ack-lost proof.
        LOGGER.trace("load generator failure summary: {} failed ops; signatures={}",
                failedOps.get(), failureSignatures);
    }

    long ackedOps() {
        return ackedOps.get();
    }

    /** DIAGNOSTIC: the server-minted opIds of every acked op (for the durable-but-unacked diff). */
    Set<String> ackedOpIds() {
        return ackedOpIds;
    }

    /** DIAGNOSTIC: number of ops that threw (and so were never acked). */
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
