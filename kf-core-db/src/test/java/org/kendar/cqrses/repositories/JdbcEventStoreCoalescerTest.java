package org.kendar.cqrses.repositories;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.bus.Context;
import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.db.AbstractJdbcTest;
import org.kendar.cqrses.db.Db;
import org.kendar.cqrses.db.InsertBuilder;
import org.kendar.cqrses.db.RowMapper;
import org.kendar.cqrses.db.UpdateBuilder;
import org.kendar.cqrses.exceptions.OptimisticConcurrencyException;
import org.kendar.cqrses.observability.Observability;
import org.kendar.cqrses.observability.ObservabilityInterface;
import org.kendar.cqrses.pg.SegmentCalculator;
import org.kendar.cqrses.utils.UUIDGenerator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The group-commit append path ({@link SegmentAppendCoalescer} +
 * {@code JdbcEventStore.writeBatch}): concurrent same-segment appends are folded
 * into one transaction with one commit, with reliability identical to the
 * one-commit-per-append shape it replaced — durable-before-ack, per-request OCC
 * isolation, individual fallback after a poisoned batch, and the gap-free
 * commit-order {@code segment_seq} invariant the pull pump depends on.
 */
class JdbcEventStoreCoalescerTest extends AbstractJdbcTest {

    private JdbcEventStore store;
    /** Every {@code onAppendBatch(requests, events)} emitted, in commit order. */
    private final List<int[]> batches = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() {
        store = new JdbcEventStore(db);
        Observability.set(new ObservabilityInterface() {
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
            public void onAppendBatch(int requests, int events) {
                batches.add(new int[]{requests, events});
            }
        });
    }

    @AfterEach
    void tearDown() {
        Observability.set(null);
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

    /** A fresh aggregate id hashing to {@code segment}. */
    private static UUID aggregateInSegment(int segment) {
        for (int i = 0; i < 100_000; i++) {
            UUID candidate = UUIDGenerator.newUuid();
            if (SegmentCalculator.calculateSegment(candidate) == segment) {
                return candidate;
            }
        }
        throw new IllegalStateException("could not find an aggregate id for segment " + segment);
    }

    /** All segment_seq values currently in {@code segment}, in tail order. */
    private List<Long> segmentSeqs(int segment) {
        return store.loadSegmentTail(segment, -1L, 1_000).stream()
                .map(InternalMessage::getSegmentSeq)
                .toList();
    }

    private static void assertContiguousFromZero(List<Long> seqs) {
        for (int i = 0; i < seqs.size(); i++) {
            assertEquals((long) i, seqs.get(i),
                    "segment_seq must be gap-free and in commit order: " + seqs);
        }
    }

    /**
     * Holds the segment_counter row lock from a side transaction, so the first
     * append blocks inside its batch write while later appends pile up in the
     * coalescer queue — making the batch composition deterministic.
     */
    private Connection lockSegmentRow(int segment) throws SQLException {
        Connection conn = db.connection();
        conn.setAutoCommit(false);
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT next_seq FROM segment_counter WHERE segment = ? FOR UPDATE")) {
            ps.setInt(1, segment);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "segment_counter row must exist before locking it");
            }
        }
        return conn;
    }

    private static void releaseSegmentRow(Connection conn) throws SQLException {
        conn.rollback();
        conn.setAutoCommit(true);
        conn.close();
    }

    @Test
    void appendsQueuedBehindABusySegmentShareOneCommit() throws Exception {
        int segment = 0;
        UUID prime = aggregateInSegment(segment);
        store.appendEvents(List.of(msg(prime, -1L, "prime"))); // counter row exists
        batches.clear();

        Connection gate = lockSegmentRow(segment);
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            // Leader: takes the coalescer slot, then blocks on the DB row lock.
            pool.submit(() -> store.appendEvents(List.of(msg(aggregateInSegment(segment), -1L, "lead"))));
            Thread.sleep(400);
            // These three arrive while the leader is stalled: they must queue and
            // later be written as ONE batch by the next leader.
            CountDownLatch queued = new CountDownLatch(3);
            for (int i = 0; i < 3; i++) {
                pool.submit(() -> {
                    queued.countDown();
                    store.appendEvents(List.of(msg(aggregateInSegment(segment), -1L, "follow")));
                });
            }
            assertTrue(queued.await(5, TimeUnit.SECONDS));
            Thread.sleep(400);
            releaseSegmentRow(gate);
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS), "all appends must complete");
        } finally {
            pool.shutdownNow();
        }

        // prime + leader + 3 followers, gap-free, in commit order.
        List<Long> seqs = segmentSeqs(segment);
        assertEquals(5, seqs.size());
        assertContiguousFromZero(seqs);
        // Two batches after the prime: the stalled leader's batch of 1, then the
        // three queued followers coalesced under a single commit.
        assertEquals(2, batches.size(), "leader batch + one coalesced batch");
        assertArrayEquals(new int[]{1, 1}, batches.get(0));
        assertArrayEquals(new int[]{3, 3}, batches.get(1));
    }

    @Test
    void occConflictInsideABatchFailsOnlyTheOffender() throws Exception {
        int segment = 0;
        UUID victim = aggregateInSegment(segment);
        store.appendEvents(List.of(msg(victim, -1L, "seed"))); // victim is at version 0
        batches.clear();

        Connection gate = lockSegmentRow(segment);
        AtomicReference<Throwable> offenderError = new AtomicReference<>();
        AtomicReference<Throwable> innocentError = new AtomicReference<>();
        ExecutorService pool = Executors.newFixedThreadPool(3);
        try {
            pool.submit(() -> store.appendEvents(List.of(msg(aggregateInSegment(segment), -1L, "lead"))));
            Thread.sleep(400);
            // Same batch: a stale-version write (expects version 99, stream is at 0)
            // and an innocent append. The innocent one must land anyway.
            pool.submit(() -> {
                try {
                    store.appendEvents(List.of(msg(victim, 99L, "stale")));
                } catch (Throwable t) {
                    offenderError.set(t);
                }
            });
            pool.submit(() -> {
                try {
                    store.appendEvents(List.of(msg(aggregateInSegment(segment), -1L, "innocent")));
                } catch (Throwable t) {
                    innocentError.set(t);
                }
            });
            Thread.sleep(400);
            releaseSegmentRow(gate);
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }

        assertInstanceOf(OptimisticConcurrencyException.class, offenderError.get(),
                "the stale-version request must fail with OCC");
        assertNull(innocentError.get(), "the innocent request must not be taken down with it");
        assertEquals(1, store.loadEvents(victim).size(), "no stale version persisted");
        // seed + lead + innocent; the failed request consumed no segment_seq (no gap).
        List<Long> seqs = segmentSeqs(segment);
        assertEquals(3, seqs.size());
        assertContiguousFromZero(seqs);
        // The follower batch carried 2 requests but wrote only the innocent's 1 event.
        assertArrayEquals(new int[]{2, 1}, batches.get(batches.size() - 1));
    }

    @Test
    void commitFailureFallsBackToIndividualAppends_noLossNoDuplicates() {
        AtomicBoolean failNextCommit = new AtomicBoolean(false);
        JdbcEventStore flaky = new JdbcEventStore(new FailingCommitDb(db, failNextCommit));

        int segment = 0;
        UUID agg = aggregateInSegment(segment);
        flaky.appendEvents(List.of(msg(agg, -1L, "prime")));

        failNextCommit.set(true);
        // The batch commit throws -> rollback -> the request is retried in its own
        // transaction (healthy connection) and must land exactly once.
        flaky.appendEvents(List.of(msg(agg, -1L, "survives")));
        assertFalse(failNextCommit.get(), "the injected failure must have fired");

        List<InternalMessage> events = flaky.loadEvents(agg);
        assertEquals(2, events.size(), "the append must survive a transient commit failure exactly once");
        assertEquals(1L, events.get(1).getContext().getAggregateVersion());
        // The rolled-back batch attempt must not have burnt a segment_seq for good:
        // the retry re-read the committed counter, so the tail stays gap-free.
        assertContiguousFromZero(segmentSeqs(segment));

        // And the store is fully healthy afterwards.
        flaky.appendEvents(List.of(msg(agg, -1L, "after")));
        assertEquals(3, flaky.loadEvents(agg).size());
    }

    @Test
    void hammeredSegmentStaysCorrect_perAggregateVersionsAndGapFreeSeqs() throws Exception {
        int segment = 0;
        int threads = 8;
        int perThread = 5;
        List<UUID> aggregates = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            aggregates.add(aggregateInSegment(segment));
        }
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int i = 0; i < threads; i++) {
                UUID agg = aggregates.get(i);
                pool.submit(() -> {
                    try {
                        go.await();
                        for (int n = 0; n < perThread; n++) {
                            store.appendEvents(List.of(msg(agg, -1L, "E" + n)));
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            go.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }

        // Per-aggregate: contiguous versions in emit order (ordering survived coalescing).
        for (UUID agg : aggregates) {
            List<InternalMessage> events = store.loadEvents(agg);
            assertEquals(perThread, events.size());
            for (int v = 0; v < perThread; v++) {
                assertEquals((long) v, events.get(v).getContext().getAggregateVersion());
                assertEquals("E" + v, events.get(v).getContext().getType());
            }
        }
        // Per-segment: every event present, segment_seq gap-free in commit order.
        List<Long> seqs = segmentSeqs(segment);
        assertEquals(threads * perThread, seqs.size());
        assertContiguousFromZero(seqs);
        long writtenInBatches = batches.stream().mapToLong(b -> b[1]).sum();
        assertEquals(threads * perThread, writtenInBatches, "every event was committed through a batch");
    }

    /**
     * A {@link Db} that delegates to the test database but hands out connections
     * whose {@code commit()} throws once when armed — the injected transient
     * failure for the fallback path. Template-method seam, no mocks (CLAUDE.md
     * testing strategy).
     */
    private static final class FailingCommitDb implements Db {
        private final Db real;
        private final AtomicBoolean failNextCommit;

        FailingCommitDb(Db real, AtomicBoolean failNextCommit) {
            this.real = real;
            this.failNextCommit = failNextCommit;
        }

        @Override
        public Connection connection() throws SQLException {
            Connection target = real.connection();
            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                    new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                        if ("commit".equals(method.getName()) && failNextCommit.compareAndSet(true, false)) {
                            throw new SQLException("injected commit failure");
                        }
                        try {
                            return method.invoke(target, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
        }

        @Override
        public void execute(String sql) {
            real.execute(sql);
        }

        @Override
        public int update(String sql, Object... args) {
            return real.update(sql, args);
        }

        @Override
        public int[] batchUpdate(String sql, List<Object[]> batches) {
            return real.batchUpdate(sql, batches);
        }

        @Override
        public <T> List<T> query(String sql, RowMapper<T> mapper, Object... args) {
            return real.query(sql, mapper, args);
        }

        @Override
        public <T> T queryForObject(String sql, RowMapper<T> mapper, Object... args) {
            return real.queryForObject(sql, mapper, args);
        }

        @Override
        public <T> T queryForObject(String sql, Class<T> type, Object... args) {
            return real.queryForObject(sql, type, args);
        }

        @Override
        public <T> List<T> queryForList(String sql, Class<T> type, Object... args) {
            return real.queryForList(sql, type, args);
        }

        @Override
        public InsertBuilder insertInto(String table) {
            return real.insertInto(table);
        }

        @Override
        public UpdateBuilder updateTable(String table) {
            return real.updateTable(table);
        }
    }
}
