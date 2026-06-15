package org.kendar.cqrses.dlq.evolved;

import org.junit.jupiter.api.Test;
import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.dlq.DlqEnqueueDecisionResult;
import org.kendar.cqrses.dlq.DlqEnqueuePolicy;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class DlqEnqueuePolicyTest {

    /** Records the (message, error) the concrete two-arg method was invoked with. */
    static class RecordingPolicy extends DlqEnqueuePolicy {
        InternalMessage seenMessage;
        final AtomicReference<Throwable> seenError = new AtomicReference<>();
        boolean called;
        private final DlqEnqueueDecisionResult result;

        RecordingPolicy(DlqEnqueueDecisionResult result) {
            this.result = result;
        }

        @Override
        public DlqEnqueueDecisionResult shouldEnqueue(InternalMessage message, Throwable error) {
            this.called = true;
            this.seenMessage = message;
            this.seenError.set(error);
            return result;
        }
    }

    @Test
    void singleArgOverloadDelegatesWithNullError() {
        var policy = new RecordingPolicy(DlqEnqueueDecisionResult.enqueue());
        var msg = new InternalMessage();

        var result = policy.shouldEnqueue(msg);

        assertTrue(policy.called);
        assertSame(msg, policy.seenMessage);
        assertNull(policy.seenError.get(), "single-arg overload must pass a null error");
        assertTrue(result.shouldEnqueue());
    }

    @Test
    void twoArgOverloadPassesErrorThrough() {
        var policy = new RecordingPolicy(DlqEnqueueDecisionResult.evict());
        var msg = new InternalMessage();
        var boom = new RuntimeException("boom");

        var result = policy.shouldEnqueue(msg, boom);

        assertSame(msg, policy.seenMessage);
        assertSame(boom, policy.seenError.get());
        assertTrue(result.shouldEvict());
    }
}
