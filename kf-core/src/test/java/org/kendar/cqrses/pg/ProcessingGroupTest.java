package org.kendar.cqrses.pg;

import org.junit.jupiter.api.Test;
import org.kendar.cqrses.bus.Bus;
import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.dlq.DlqStore;
import org.kendar.cqrses.serialization.MessageSerializer;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ProcessingGroupTest {
    Bus bus = null;
    MessageSerializer serializer = null;
    Boolean commandSide = false;
    DlqStore dlqStore = null;
    Bus.ProcessingGroupPolicyConfig policy = null;
    Map<Class<?>, List<Bus.Registration>> consumer =null;
    @Test
    void defaultsToRunningWithEmptyQueueAndNullThread() {

        ProcessingGroup pg = new ProcessingGroup("g", bus, serializer, commandSide, dlqStore, consumer, policy);
        assertEquals("g", pg.getName());
        assertTrue(pg.isRunning());
        assertNotNull(pg.getQueue());
        assertTrue(pg.getQueue().isEmpty());
        assertNull(pg.getThread());
    }

    @Test
    void setRunningTogglesFlag() {
        ProcessingGroup pg = new ProcessingGroup("g", bus, serializer, commandSide, dlqStore, consumer, policy);
        pg.setRunning(false);
        assertFalse(pg.isRunning());
        pg.setRunning(true);
        assertTrue(pg.isRunning());
    }

    @Test
    void queueAcceptsMessages() {
        ProcessingGroup pg = new ProcessingGroup("g", bus, serializer, commandSide, dlqStore, consumer, policy);
        InternalMessage m = new InternalMessage();
        LaneWork work = new LaneWork(m, List.of());
        pg.getQueue().add(work);
        assertEquals(1, pg.getQueue().size());
        assertSame(work, pg.getQueue().poll());
    }

    @Test
    void threadAndNameSettersWork() {
        ProcessingGroup pg = new ProcessingGroup("a", bus, serializer, commandSide, dlqStore, consumer, policy);
        Thread t = new Thread(() -> {
        });
        pg.setThread(t);
        pg.setName("b");
        assertSame(t, pg.getThread());
        assertEquals("b", pg.getName());
    }
}
