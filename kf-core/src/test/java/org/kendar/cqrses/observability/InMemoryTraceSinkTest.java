package org.kendar.cqrses.observability;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryTraceSinkTest {

    private static PerfTrace trace(String type) {
        return new PerfTrace(UUID.randomUUID(), type, UUID.randomUUID(),
                System.currentTimeMillis(), true, List.of(new PerfStage("total", 1, 0)));
    }

    @Test
    void keepsAllBelowCapacity() {
        var sink = new InMemoryTraceSink(10);
        for (int i = 0; i < 5; i++) {
            sink.accept(trace("t" + i));
        }
        assertEquals(5, sink.snapshot().size());
        assertEquals(0, sink.dropped());
    }

    @Test
    void overflowDropsOldestAndCountsEvictions() {
        var sink = new InMemoryTraceSink(3);
        for (int i = 0; i < 5; i++) {
            sink.accept(trace("t" + i));
        }
        var kept = sink.snapshot().stream().map(PerfTrace::commandType).toList();
        assertEquals(List.of("t2", "t3", "t4"), kept);
        assertEquals(2, sink.dropped());
    }

    @Test
    void snapshotIsImmutable() {
        var sink = new InMemoryTraceSink(3);
        sink.accept(trace("a"));
        var snap = sink.snapshot();
        assertThrows(UnsupportedOperationException.class, () -> snap.add(trace("b")));
    }

    @Test
    void clearResetsBufferAndDropCount() {
        var sink = new InMemoryTraceSink(1);
        sink.accept(trace("a"));
        sink.accept(trace("b"));
        assertEquals(1, sink.dropped());
        sink.clear();
        assertTrue(sink.snapshot().isEmpty());
        assertEquals(0, sink.dropped());
    }

    @Test
    void nullTraceIgnored() {
        var sink = new InMemoryTraceSink(1);
        sink.accept(null);
        assertTrue(sink.snapshot().isEmpty());
    }

    @Test
    void concurrentAcceptNeverLosesMoreThanCapacityAllows() throws Exception {
        var sink = new InMemoryTraceSink(64);
        var threads = new Thread[4];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < 500; j++) {
                    sink.accept(trace("x"));
                }
            });
            threads[i].start();
        }
        for (var thread : threads) {
            thread.join();
        }
        assertEquals(64, sink.snapshot().size());
        // every trace not in the buffer was evicted by exactly one counted poll
        assertEquals(2000 - 64, sink.dropped());
    }
}
