package org.kendar.cqrses.pg;

import org.kendar.cqrses.bus.Bus;
import org.kendar.cqrses.bus.EventBus;
import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.scheduler.Sleeper;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One thread per saga group. Resolves each incoming event to a sagaId via the
 * correlation index, then fans it out to the {@code segment(sagaId)} worker lane
 * carrying the single matching registration. {@code @SagaStart} creations are run
 * inline on the resolver thread (serialized) via a thread-less create-PG, because
 * the {@code @SagaId} is assigned inside the handler and the create cannot be
 * routed by {@code segment(sagaId)} up front.
 */
public class SagaResolver {
    private static final Logger LOGGER = LoggerFactory.getLogger(SagaResolver.class);
    private final String group;
    private final EventBus bus;
    private final MessageSerializer serializer;
    private final Map<Class<?>, List<Bus.Registration>> consumers;
    private final ProcessingGroup[] lanes;
    private final ProcessingGroup createPg;
    private final ConcurrentLinkedQueue<InternalMessage> queue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private Thread thread;

    public SagaResolver(String group, EventBus bus, MessageSerializer serializer,
                        Map<Class<?>, List<Bus.Registration>> consumers,
                        ProcessingGroup[] lanes, ProcessingGroup createPg) {
        this.group = group;
        this.bus = bus;
        this.serializer = serializer;
        this.consumers = consumers;
        this.lanes = lanes;
        this.createPg = createPg;
    }

    public ConcurrentLinkedQueue<InternalMessage> getQueue() {
        return queue;
    }

    public boolean isRunning() {
        return running.get();
    }

    public void setRunning(boolean running) {
        this.running.set(running);
    }

    public Thread getThread() {
        return thread;
    }

    public void setThread(Thread thread) {
        this.thread = thread;
    }

    public void start() {
        while (true) {
            if (!isRunning()) return;
            InternalMessage msg;
            while ((msg = queue.poll()) != null) {
                if (!isRunning()) return;
                try {
                    resolve(msg);
                } catch (Exception e) {
                    // Mirror the ProcessingGroup lane-thread contract: a single bad
                    // event must not silently kill the saga resolver thread (which
                    // would otherwise stop the whole saga group). Log and skip.
                    LOGGER.error("Error resolving saga event in group " + group, e);
                }
            }
            Sleeper.yield();
        }
    }

    // TODO: 10-min correlation→sagaId cache (à la Axon saga cache).
    private void resolve(InternalMessage msg) {
        var messageType = bus.getMessageClass(msg.getContext().getType());
        if (messageType == null) return;
        var regs = consumers.get(messageType);
        if (regs == null || regs.isEmpty()) return;
        Object event = serializer.deserialize(msg.getPayload(), messageType);
        for (var reg : regs) {
            var sagaId = bus.resolveSagaId(event, reg);
            if (sagaId.isPresent()) {
                int seg = SegmentCalculator.calculateSegment(sagaId.get());
                lanes[seg].getQueue().add(new LaneWork(msg, java.util.List.of(reg)));
            } else if (isSagaStart(reg)) {
                // @SagaId is set inside the @SagaStart handler, so the create cannot be
                // routed by segment(sagaId) up front: run it inline on THIS resolver
                // thread (serialized) via the thread-less createPg, which stores the
                // saga synchronously so the next event resolves to a lane.
                createPg.invokeConsumers(msg, java.util.List.of(reg));
            }
            // empty + not @SagaStart → skip (event is not for any saga of this type)
        }
    }

    private static boolean isSagaStart(Bus.Registration reg) {
        return reg.methodInfo() != null
            && reg.methodInfo().getAnnotation(org.kendar.cqrses.annotations.SagaStart.class) != null;
    }

    /**
     * Cluster pull-mode resolve, scoped to one OWNED segment and run synchronously
     * on the caller's (saga worker) thread — there are no lane/resolver threads in
     * pull mode. Honors "all by {@code segment(sagaId)} except creation":
     * <ul>
     *   <li><b>Update</b>: correlate to an existing sagaId via the durable
     *       correlation index; run it only if {@code segment(sagaId) == ownedSegment}
     *       (else another segment's owner handles it).</li>
     *   <li><b>Create</b> ({@code @SagaStart}, no correlation): key on
     *       {@code segment(event.aggregateId)}; run it only if that equals
     *       {@code ownedSegment}. Exactly one owner per segment ⇒ no duplicate
     *       creation.</li>
     * </ul>
     * Each owned-segment worker invokes only {@code lanes[ownedSegment]}, so workers
     * for the same group touch disjoint lanes; {@code createPg} is invoked through
     * per-thread state only, so concurrent creates are safe.
     */
    public void resolveForSegment(InternalMessage msg, int ownedSegment) {
        var messageType = bus.getMessageClass(msg.getContext().getType());
        if (messageType == null) return;
        var regs = consumers.get(messageType);
        if (regs == null || regs.isEmpty()) return;
        Object event = serializer.deserialize(msg.getPayload(), messageType);
        for (var reg : regs) {
            var sagaId = bus.resolveSagaId(event, reg);
            if (sagaId.isPresent()) {
                int seg = SegmentCalculator.calculateSegment(sagaId.get());
                if (seg == ownedSegment) {
                    lanes[ownedSegment].invokeConsumers(msg, List.of(reg)); // synchronous update
                }
            } else if (isSagaStart(reg)) {
                UUID aggId = msg.getContext().getAggregateId();
                int seg = (aggId == null) ? -1 : SegmentCalculator.calculateSegment(aggId);
                if (seg == ownedSegment) {
                    createPg.invokeConsumers(msg, List.of(reg)); // synchronous create
                }
            }
            // empty + not @SagaStart → skip (event not for any saga of this type)
        }
    }
}
