package org.kendar.cqrses.observability;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TraceRecorderTest {

    private static final class RecordingSink implements TraceSink {
        final List<PerfTrace> traces = new CopyOnWriteArrayList<>();

        @Override
        public void accept(PerfTrace trace) {
            traces.add(trace);
        }
    }

    @AfterEach
    void tearDown() {
        TraceRecorder.reset();
    }

    @Test
    void disabledIsNoOp() {
        assertFalse(TraceRecorder.begin(UUID.randomUUID(), "Cmd", UUID.randomUUID()));
        TraceRecorder.stage("handler", 1);
        TraceRecorder.end(true); // must not throw
    }

    @Test
    void sampleEveryOneTracesEveryCommand() {
        var sink = new RecordingSink();
        TraceRecorder.install(sink, 1);
        for (int i = 0; i < 5; i++) {
            assertTrue(TraceRecorder.begin(UUID.randomUUID(), "Cmd", UUID.randomUUID()));
            TraceRecorder.end(true);
        }
        assertEquals(5, sink.traces.size());
    }

    @Test
    void sampleEveryThreeIsExactlyOneInThree() {
        var sink = new RecordingSink();
        TraceRecorder.install(sink, 3);
        int sampled = 0;
        for (int i = 0; i < 300; i++) {
            if (TraceRecorder.begin(UUID.randomUUID(), "Cmd", UUID.randomUUID())) {
                sampled++;
                TraceRecorder.end(true);
            }
        }
        assertEquals(100, sampled);
        assertEquals(100, sink.traces.size());
    }

    @Test
    void stagesArriveInOrderWithSyntheticTotalLast() {
        var sink = new RecordingSink();
        TraceRecorder.install(sink, 1);
        var traceId = UUID.randomUUID();
        var aggId = UUID.randomUUID();
        assertTrue(TraceRecorder.begin(traceId, "RecordOperation", aggId));
        TraceRecorder.stage("rehydrate.load", 100, 7);
        TraceRecorder.stage("handler", 200);
        TraceRecorder.stage("append.commit", 300);
        TraceRecorder.end(true);

        assertEquals(1, sink.traces.size());
        var trace = sink.traces.getFirst();
        assertEquals(traceId, trace.traceId());
        assertEquals("RecordOperation", trace.commandType());
        assertEquals(aggId, trace.aggregateId());
        assertTrue(trace.ok());
        assertEquals(List.of("rehydrate.load", "handler", "append.commit", "total"),
                trace.stages().stream().map(PerfStage::stage).toList());
        assertEquals(7, trace.stages().getFirst().detail());
        // total's detail = number of recorded stages before it
        assertEquals(3, trace.stages().getLast().detail());
        assertTrue(trace.stages().getLast().nanos() >= 0);
    }

    @Test
    void failurePropagatesOkFalse() {
        var sink = new RecordingSink();
        TraceRecorder.install(sink, 1);
        assertTrue(TraceRecorder.begin(UUID.randomUUID(), "Cmd", UUID.randomUUID()));
        TraceRecorder.end(false);
        assertFalse(sink.traces.getFirst().ok());
    }

    @Test
    void endClearsThreadLocal() {
        var sink = new RecordingSink();
        TraceRecorder.install(sink, 1);
        assertTrue(TraceRecorder.begin(UUID.randomUUID(), "Cmd", UUID.randomUUID()));
        TraceRecorder.end(true);
        TraceRecorder.stage("stale", 1);
        TraceRecorder.end(true); // no active trace: no-op
        assertEquals(1, sink.traces.size());
    }

    @Test
    void beginClearsStaleLeftoverTrace() {
        var sink = new RecordingSink();
        TraceRecorder.install(sink, 1);
        // a command that died between begin and end
        assertTrue(TraceRecorder.begin(UUID.randomUUID(), "Dead", UUID.randomUUID()));
        TraceRecorder.stage("handler", 1);
        // next command on the same thread must start clean
        assertTrue(TraceRecorder.begin(UUID.randomUUID(), "Fresh", UUID.randomUUID()));
        TraceRecorder.end(true);
        assertEquals(1, sink.traces.size());
        assertEquals("Fresh", sink.traces.getFirst().commandType());
        assertEquals(List.of("total"), sink.traces.getFirst().stages().stream().map(PerfStage::stage).toList());
    }

    @Test
    void throwingSinkNeverFailsTheCommand() {
        TraceRecorder.install(t -> {
            throw new IllegalStateException("bad sink");
        }, 1);
        assertTrue(TraceRecorder.begin(UUID.randomUUID(), "Cmd", UUID.randomUUID()));
        TraceRecorder.end(true); // must not throw
    }

    @Test
    void concurrentThreadsDoNotCrossContaminate() throws Exception {
        var sink = new RecordingSink();
        TraceRecorder.install(sink, 1);
        var errors = new CopyOnWriteArrayList<Throwable>();
        var threads = new ArrayList<Thread>();
        for (int i = 0; i < 4; i++) {
            var stageName = "stage-" + i;
            var thread = new Thread(() -> {
                for (int j = 0; j < 50; j++) {
                    assertTrue(TraceRecorder.begin(UUID.randomUUID(), stageName, UUID.randomUUID()));
                    TraceRecorder.stage(stageName, 1);
                    TraceRecorder.end(true);
                }
            });
            thread.setUncaughtExceptionHandler((t, e) -> errors.add(e));
            threads.add(thread);
            thread.start();
        }
        for (var thread : threads) {
            thread.join();
        }
        assertTrue(errors.isEmpty(), errors.toString());
        assertEquals(200, sink.traces.size());
        for (var trace : sink.traces) {
            // every stage in a trace belongs to the thread that began it
            assertEquals(2, trace.stages().size());
            assertEquals(trace.commandType(), trace.stages().getFirst().stage());
        }
    }

    @Test
    void distinctTraceIdsAcrossThreads() {
        var sink = new RecordingSink();
        TraceRecorder.install(sink, 1);
        assertTrue(TraceRecorder.begin(UUID.randomUUID(), "A", UUID.randomUUID()));
        TraceRecorder.end(true);
        assertTrue(TraceRecorder.begin(UUID.randomUUID(), "B", UUID.randomUUID()));
        TraceRecorder.end(true);
        assertNotEquals(sink.traces.get(0).traceId(), sink.traces.get(1).traceId());
    }
}
