package org.kendar.cqrses.repositories;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.bus.Context;
import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.db.AbstractJdbcTest;
import org.kendar.cqrses.db.ConnectionStorage;
import org.kendar.cqrses.observability.AppendPhase;
import org.kendar.cqrses.observability.NullObservability;
import org.kendar.cqrses.observability.Observability;
import org.kendar.cqrses.observability.ObservabilityInterface;
import org.kendar.cqrses.observability.PerfStage;
import org.kendar.cqrses.observability.PerfTrace;
import org.kendar.cqrses.observability.TraceRecorder;
import org.kendar.cqrses.pg.SegmentCalculator;
import org.kendar.cqrses.utils.UUIDGenerator;

import java.sql.Connection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The append-phase instrumentation contract: which phases fire, in which
 * order, and that observability never changes append behaviour
 * (NullObservability parity). See AppendPhase / ObservabilityInterface.
 */
class JdbcEventStoreObservabilityTest extends AbstractJdbcTest {

    private JdbcEventStore store;
    private final List<String> phases = new CopyOnWriteArrayList<>();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger inFlightMax = new AtomicInteger();

    private final ObservabilityInterface recording = new ObservabilityInterface() {
        @Override
        public void onCommandHandled(String group, String commandType, long nanos, boolean ok) {
        }

        @Override
        public void onAggregateRehydrated(String aggregateType, int eventsReplayed, long nanos) {
        }

        @Override
        public void onEventsAppended(int count, long nanos) {
        }

        @Override
        public void onEventDispatched(String group, int segment, String eventType, long nanos, boolean ok) {
        }

        @Override
        public void onSagaDispatched(String group, int segment, String eventType, long nanos, boolean ok) {
        }

        @Override
        public void onSegmentTailRead(String group, int eventsRead, long nanos) {
        }

        @Override
        public void onCheckpointSaved(String group, int segment) {
        }

        @Override
        public void onDlqEnqueued(String group, String eventType) {
        }

        @Override
        public void onSqlExecuted(String category, long nanos, boolean ok) {
        }

        @Override
        public void onAppendPhase(String phase, long nanos) {
            phases.add(phase);
        }

        @Override
        public void onAppendInFlight(int segment, int delta) {
            int now = inFlight.addAndGet(delta);
            inFlightMax.accumulateAndGet(now, Math::max);
        }
    };

    @BeforeEach
    void setUp() {
        store = new JdbcEventStore(db);
        Observability.set(recording);
    }

    @AfterEach
    void tearDown() {
        Observability.set(null);
        TraceRecorder.reset();
    }

    private static InternalMessage msg(UUID aggregateId, long version, String type) {
        InternalMessage m = new InternalMessage();
        Context c = new Context();
        c.setAggregateId(aggregateId);
        c.setAggregateVersion(version);
        c.setType(type);
        m.setContext(c);
        m.setPayload(new byte[]{1, 2, 3});
        return m;
    }

    @Test
    void ownConnectionAppendEmitsAllPhasesInOrder() {
        UUID agg = UUIDGenerator.newUuid();
        store.appendEvents(List.of(msg(agg, 0, "A"), msg(agg, 1, "B"), msg(agg, 2, "C")));
        assertEquals(List.of(AppendPhase.CONN, AppendPhase.LOCK, AppendPhase.CURRENT_MAX,
                        AppendPhase.INSERT, AppendPhase.COUNTER, AppendPhase.COMMIT),
                phases);
        assertEquals(0, inFlight.get(), "in-flight must return to 0");
        assertEquals(1, inFlightMax.get());
    }

    @Test
    void boundConnectionAppendEmitsNoCommitPhase() throws Exception {
        Connection boundary = db.connection();
        boundary.setAutoCommit(false);
        ConnectionStorage.bind(boundary);
        try {
            UUID agg = UUIDGenerator.newUuid();
            store.appendEvents(List.of(msg(agg, 0, "A")));
            assertEquals(List.of(AppendPhase.CONN, AppendPhase.LOCK, AppendPhase.CURRENT_MAX,
                            AppendPhase.INSERT, AppendPhase.COUNTER),
                    phases, "the boundary owns the commit; the store must not time one");
            boundary.commit();
        } finally {
            ConnectionStorage.unbind();
            boundary.setAutoCommit(true);
            boundary.close();
        }
        assertEquals(0, inFlight.get());
    }

    @Test
    void occViolationStopsPhasesWhereItThrewAndInFlightReturnsToZero() {
        UUID agg = UUIDGenerator.newUuid();
        store.appendEvents(List.of(msg(agg, 0, "A")));
        phases.clear();
        assertThrows(IllegalStateException.class,
                () -> store.appendEvents(List.of(msg(agg, 0, "dup"))));
        // The version check throws inside the insert loop: insert/counter/commit never fire.
        assertEquals(List.of(AppendPhase.CONN, AppendPhase.LOCK, AppendPhase.CURRENT_MAX), phases);
        assertEquals(0, inFlight.get(), "in-flight must return to 0 on the failure path");
    }

    @Test
    void sampledTraceCarriesAppendStagesWithInsertDetail() {
        List<PerfTrace> traces = new CopyOnWriteArrayList<>();
        TraceRecorder.install(traces::add, 1);
        UUID agg = UUIDGenerator.newUuid();
        assertTrue(TraceRecorder.begin(UUIDGenerator.newUuid(), "Cmd", agg));
        store.appendEvents(List.of(msg(agg, 0, "A"), msg(agg, 1, "B")));
        TraceRecorder.end(true);

        assertEquals(1, traces.size());
        var stages = traces.getFirst().stages();
        assertEquals(List.of("append.conn", "append.lock", "append.currentMax",
                        "append.insert", "append.counter", "append.commit", "append.wait", "total"),
                stages.stream().map(PerfStage::stage).toList());
        var insert = stages.stream().filter(s -> s.stage().equals("append.insert")).findFirst().orElseThrow();
        assertEquals(2, insert.detail(), "insert detail = events in the batch");
        var wait = stages.stream().filter(s -> s.stage().equals("append.wait")).findFirst().orElseThrow();
        assertEquals(1, wait.detail(), "an uncontended append leads its own batch (detail 1 = led)");
    }

    @Test
    void unsampledCommandRecordsNoTraceStages() {
        List<PerfTrace> traces = new CopyOnWriteArrayList<>();
        TraceRecorder.install(traces::add, Integer.MAX_VALUE);
        UUID agg = UUIDGenerator.newUuid();
        // first begin is sampled (counter starts at 0) — burn it on another thread-less begin/end
        TraceRecorder.begin(UUIDGenerator.newUuid(), "Burn", agg);
        TraceRecorder.end(true);
        // this one is 1-in-MAX_VALUE: not sampled
        assertEquals(false, TraceRecorder.begin(UUIDGenerator.newUuid(), "Cmd", agg));
        store.appendEvents(List.of(msg(agg, 0, "A")));
        TraceRecorder.end(true);
        assertEquals(1, traces.size(), "only the burnt sampled trace");
        assertEquals("Burn", traces.getFirst().commandType());
    }

    @Test
    void appendResultsIdenticalUnderNullAndRecordingObservability() {
        UUID withRecording = UUIDGenerator.newUuid();
        store.appendEvents(List.of(msg(withRecording, -1L, "A"), msg(withRecording, -1L, "B")));

        Observability.set(new NullObservability());
        UUID withNull = UUIDGenerator.newUuid();
        store.appendEvents(List.of(msg(withNull, -1L, "A"), msg(withNull, -1L, "B")));

        var a = store.loadEvents(withRecording);
        var b = store.loadEvents(withNull);
        assertEquals(a.size(), b.size());
        for (int i = 0; i < a.size(); i++) {
            assertEquals(a.get(i).getContext().getAggregateVersion(),
                    b.get(i).getContext().getAggregateVersion());
            assertEquals(a.get(i).getContext().getType(), b.get(i).getContext().getType());
        }
        int segA = SegmentCalculator.calculateSegment(withRecording);
        assertTrue(store.loadSegmentTail(segA, -1L, 100).size() >= 2);
    }
}
