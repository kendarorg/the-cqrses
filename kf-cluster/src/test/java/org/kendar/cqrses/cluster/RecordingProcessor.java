package org.kendar.cqrses.cluster;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test {@link ItemProcessor} that records which partitions it is actively pumping and which it has
 * been asked to stop. {@link #process(int)} blocks (as a real never-returning pump would) until the
 * partition is asked to stop, so the test can assert "is partition i being processed right now".
 */
final class RecordingProcessor implements ItemProcessor {

    private final Set<Integer> processing = ConcurrentHashMap.newKeySet();
    private final Set<Integer> stopRequested = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<Integer, CountDownLatch> stopLatches = new ConcurrentHashMap<>();
    private final AtomicInteger processStarts = new AtomicInteger();

    @Override
    public void process(int itemId) {
        processStarts.incrementAndGet();
        CountDownLatch stop = stopLatches.computeIfAbsent(itemId, k -> new CountDownLatch(1));
        processing.add(itemId);
        try {
            stop.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            processing.remove(itemId);
        }
    }

    @Override
    public void stopProcess(int itemId) {
        stopRequested.add(itemId);
        stopLatches.computeIfAbsent(itemId, k -> new CountDownLatch(1)).countDown();
    }

    boolean isProcessing(int itemId) {
        return processing.contains(itemId);
    }

    boolean stopRequested(int itemId) {
        return stopRequested.contains(itemId);
    }

    int processStarts() {
        return processStarts.get();
    }

    /** Spin-wait (bounded) until partition {@code itemId} is actively processing. */
    boolean awaitProcessing(int itemId, long timeoutMs) {
        return await(() -> isProcessing(itemId), timeoutMs);
    }

    static boolean await(java.util.function.BooleanSupplier cond, long timeoutMs) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < deadline) {
            if (cond.getAsBoolean()) {
                return true;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return cond.getAsBoolean();
    }
}
