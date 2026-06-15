package org.kendar.cqrses.dlq;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.bus.Bus;
import org.kendar.cqrses.bus.Context;
import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.bus.StubBuses;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.exceptions.DlqRetryFailedException;
import org.kendar.cqrses.pg.ProcessingGroupsManager;
import org.kendar.cqrses.serialization.JacksonMessageSerializer;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.serialization.UpcastersManager;
import org.kendar.cqrses.utils.TriFunction;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class LocalDlqManagerTest {

    private static final String SEQ = "seq-1";
    private static final String GROUP = "fraud";

    private FakeDlqStore store;
    private RetryTestBus bus;
    private AtomicInteger invocations;
    private int failFromInvocation; // handler throws on the Nth invocation onward (1-based); 0 = never
    private LocalDlqManager manager;
    private UpcastersManager upcasterManager;

    static class Holder {
    }

    @BeforeEach
    void setUp() {
        upcasterManager = new UpcastersManager(new JacksonMessageSerializer(),List.of());
        GlobalRegistry.register(UpcastersManager.class, upcasterManager);
        store = new FakeDlqStore();
        invocations = new AtomicInteger();
        failFromInvocation = 1; // fail by default
        TriFunction<Object, Object, Context, Object> handler = (t, m, c) -> {
            int n = invocations.incrementAndGet();
            if (failFromInvocation != 0 && n >= failFromInvocation) {
                throw new IllegalStateException("still failing");
            }
            return null;
        };
        var registration = new Bus.Registration(Holder.class, handler,
                Bus.defaultProcessingGroupPolicyConfig(GROUP), null);
        bus = new RetryTestBus(new FixedSerializer(new Object()));
        bus.groupConsumers = Map.of(Object.class, List.of(registration));
        bus.target = "the-target";
        manager = new LocalDlqManager(bus, store, null);
    }

    private DlqItem deadLetter(String seq) {
        var item = new DlqItem();
        item.setId(UUID.randomUUID());
        item.setSequenceId(seq);
        item.setProcessingGroup(GROUP);
        item.setEventType("Deposited");
        item.setStatus(DlqItemStatus.PENDING);
        item.setProcessingContext(new Context());
        item.setSerializedEvent(new byte[]{1, 2, 3});
        store.addItem(item, seq);
        return item;
    }

    // ── retry ────────────────────────────────────────────────────────────────

    @Test
    void retrySuccessResolvesAndClearsBlock() {
        failFromInvocation = 0; // never fail
        var letter = deadLetter(SEQ);

        manager.retry(letter.getId());

        assertEquals(1, invocations.get(), "handler re-invoked exactly once");
        assertEquals(DlqItemStatus.RESOLVED, letter.getStatus());
        assertTrue(store.getItem(letter.getId()).isEmpty(), "resolved item removed from store");
        assertFalse(store.hasBlockedItems(SEQ), "block cleared");
        assertTrue(store.removed.contains(letter.getId()));
    }

    @Test
    void retryFailureRecordsAttemptThrowsAndStaysBlocking() {
        var letter = deadLetter(SEQ);

        var ex = assertThrows(DlqRetryFailedException.class, () -> manager.retry(letter.getId()));
        assertEquals(letter.getId(), ex.getItemId());
        assertEquals(IllegalStateException.class.getName(), ex.getFailedErrorClass());

        assertEquals(1, letter.getRetryCount());
        assertEquals(DlqItemStatus.PENDING, letter.getStatus(), "returns to PENDING after a failed retry");
        assertEquals(IllegalStateException.class.getName(), letter.getLastRetryErrorClass());
        assertEquals("still failing", letter.getLastRetryErrorMessage());
        assertNotNull(letter.getLastRetryAt());
        assertTrue(store.getItem(letter.getId()).isPresent(), "failed item stays in the store");
        assertTrue(store.hasBlockedItems(SEQ), "still blocking");
        assertFalse(store.removed.contains(letter.getId()));
    }

    @Test
    void retryByUnknownIdThrows() {
        assertThrows(IllegalArgumentException.class, () -> manager.retry(UUID.randomUUID()));
    }

    @Test
    void retryRejectsResolvedDismissedOrRetryingItems() {
        var resolved = deadLetter("a");
        resolved.setStatus(DlqItemStatus.RESOLVED);
        assertThrows(IllegalStateException.class, () -> manager.retry(resolved.getId()));

        var dismissed = deadLetter("b");
        dismissed.setStatus(DlqItemStatus.DISMISSED);
        assertThrows(IllegalStateException.class, () -> manager.retry(dismissed.getId()));

        var retrying = deadLetter("c");
        retrying.setStatus(DlqItemStatus.RETRYING);
        assertThrows(IllegalStateException.class, () -> manager.retry(retrying.getId()));
    }

    @Test
    void retryRejectsItemWithoutProcessingContext() {
        var letter = deadLetter(SEQ);
        letter.setProcessingContext(null);
        assertThrows(IllegalStateException.class, () -> manager.retry(letter.getId()));
        assertEquals(0, invocations.get());
    }

    @Test
    void retryRejectsUnknownEventType() {
        bus.messageType = null; // getMessageClass returns null
        var letter = deadLetter(SEQ);
        assertThrows(IllegalStateException.class, () -> manager.retry(letter.getId()));
    }

    @Test
    void retryBySequenceProcessesFifoAndStopsAtFirstFailure() {
        failFromInvocation = 2; // first retry succeeds, second fails
        var first = deadLetter(SEQ);
        var second = deadLetter(SEQ);

        assertThrows(DlqRetryFailedException.class, () -> manager.retry(SEQ));

        assertEquals(2, invocations.get());
        assertTrue(store.getItem(first.getId()).isEmpty(), "first resolved and removed");
        assertEquals(DlqItemStatus.RESOLVED, first.getStatus());
        assertTrue(store.getItem(second.getId()).isPresent(), "second left blocking");
        assertEquals(1, second.getRetryCount());
        assertEquals(DlqItemStatus.PENDING, second.getStatus());
    }

    // ── dismiss ──────────────────────────────────────────────────────────────

    @Test
    void dismissMarksDismissedAndClearsBlockWithoutInvoking() {
        var letter = deadLetter(SEQ);

        manager.dismiss(letter.getId());

        assertEquals(0, invocations.get(), "dismiss never re-invokes the handler");
        assertEquals(DlqItemStatus.DISMISSED, letter.getStatus());
        assertTrue(store.getItem(letter.getId()).isEmpty());
        assertFalse(store.hasBlockedItems(SEQ));
    }

    @Test
    void dismissBySequenceClearsAllItems() {
        deadLetter(SEQ);
        deadLetter(SEQ);

        manager.dismiss(SEQ);

        assertFalse(store.hasBlockedItems(SEQ));
        assertEquals(0, invocations.get());
    }

    // ── redispatch ───────────────────────────────────────────────────────────

    @Test
    void redispatchClearsBlockAndReEnqueuesOntoLiveWorker() {
        var captured = new ArrayList<InternalMessage>();
        var capturedGroups = new ArrayList<Set<String>>();
        var liveManager = new ProcessingGroupsManager(bus, bus.getSerializer(), store) {
            @Override
            public void send(Set<String> pgs, InternalMessage command) {
                capturedGroups.add(pgs);
                captured.add(command);
            }
        };
        var mgr = new LocalDlqManager(bus, store, liveManager);
        var letter = deadLetter(SEQ);

        mgr.redispatch(letter.getId());

        assertEquals(0, invocations.get(), "redispatch hands off to the live worker, not a direct invoke");
        assertTrue(store.getItem(letter.getId()).isEmpty(), "item removed to clear the block");
        assertFalse(store.hasBlockedItems(SEQ));
        assertEquals(1, captured.size());
        assertTrue(captured.get(0).isEvent());
        assertTrue(captured.get(0).isRetry());
        assertArrayEquals(new byte[]{1, 2, 3}, captured.get(0).getPayload());
        assertEquals(Set.of(GROUP), capturedGroups.get(0));
    }

    @Test
    void redispatchWithoutLiveManagerThrows() {
        var letter = deadLetter(SEQ);
        assertThrows(IllegalStateException.class, () -> manager.redispatch(letter.getId()));
    }

    // ── construction guard ───────────────────────────────────────────────────

    @Test
    void constructingOverACommandBusIsRejected() {
        assertThrows(UnsupportedOperationException.class,
                () -> new LocalDlqManager(StubBuses.noopCommandBus(), store, null));
    }

    // ── fakes ────────────────────────────────────────────────────────────────

    /** Bus that feeds the manager controlled consumers/target/message-class. */
    static class RetryTestBus extends Bus {
        Map<Class<?>, List<Registration>> groupConsumers;
        Class<?> messageType = Object.class;
        Object target;

        RetryTestBus(MessageSerializer serializer) {
            super(serializer);
        }

        @Override
        public Map<Class<?>, List<Registration>> getConsumers(String group) {
            return groupConsumers;
        }

        @Override
        public Class<?> getMessageClass(String name) {
            return messageType;
        }

        @Override
        public Object findTarget(Object message, Registration registration) {
            return target;
        }

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        public void clear() {
        }

        @Override
        protected boolean registerInternal(Class<?> handlerClass) {
            return false;
        }
    }

    static class FixedSerializer implements MessageSerializer<Object,Object> {
        private final Object value;

        FixedSerializer(Object value) {
            this.value = value;
        }

        @Override
        public byte[] serialize(Object domainObject) {
            return new byte[0];
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T deserialize(byte[] payload, Class<T> targetClass) {
            return (T) value;
        }

        @Override
        public Object serializeToFormat(Object domainObject) {
            return domainObject;
        }

        @Override
        public <T> T deserializeFromFormat(Object payload, Class<T> targetClass) {
            return (T)payload;
        }

        @Override
        public Object deserializeToFormat(byte[] payload) {
            return payload;
        }

        @Override
        public Object deserializeToIntermediate(byte[] payload) {
            return payload;
        }
    }

    /** FIFO-per-sequence + by-id index, mirroring InMemoryDlqStore semantics. */
    static class FakeDlqStore implements DlqStore {
        final Map<String, Deque<DlqItem>> queues = new LinkedHashMap<>();
        final Map<UUID, DlqItem> index = new LinkedHashMap<>();
        final List<UUID> removed = new ArrayList<>();

        @Override
        public boolean hasBlockedItems(String sequenceId) {
            var q = queues.get(sequenceId);
            return q != null && !q.isEmpty();
        }

        @Override
        public void evictFirst(String sequenceId) {
            var q = queues.get(sequenceId);
            if (q != null && !q.isEmpty()) index.remove(q.pollFirst().getId());
        }

        @Override
        public void addItem(DlqItem item, String sequenceId) {
            queues.computeIfAbsent(sequenceId, k -> new ArrayDeque<>()).addLast(item);
            index.put(item.getId(), item);
        }

        @Override
        public List<DlqItem> listItems(String sequenceId) {
            var q = queues.get(sequenceId);
            return q == null ? List.of() : List.copyOf(q);
        }

        @Override
        public Optional<DlqItem> getItem(UUID id) {
            return Optional.ofNullable(index.get(id));
        }

        @Override
        public void updateStatus(UUID id, DlqItemStatus status) {
            var i = index.get(id);
            if (i != null) i.setStatus(status);
        }

        @Override
        public void updateItem(DlqItem item) {
            // same reference is already in the queue; nothing to reorder
        }

        @Override
        public void removeItem(UUID id) {
            removed.add(id);
            var i = index.remove(id);
            if (i != null) {
                var q = queues.get(i.getSequenceId());
                if (q != null) q.remove(i);
            }
        }

        @Override
        public void clear() {
            queues.clear();
            index.clear();
            removed.clear();
        }
    }
}
