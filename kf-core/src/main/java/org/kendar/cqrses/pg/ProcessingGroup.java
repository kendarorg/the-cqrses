package org.kendar.cqrses.pg;

import org.kendar.cqrses.annotations.Event;
import org.kendar.cqrses.annotations.Saga;
import org.kendar.cqrses.bus.*;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.di.TargetType;
import org.kendar.cqrses.dlq.DlqItem;
import org.kendar.cqrses.dlq.DlqItemStatus;
import org.kendar.cqrses.dlq.DlqStore;
import org.kendar.cqrses.exceptions.InvalidHandlerException;
import org.kendar.cqrses.exceptions.OptimisticConcurrencyException;
import org.kendar.cqrses.observability.Observability;
import org.kendar.cqrses.observability.TraceRecorder;
import org.kendar.cqrses.repositories.EventStore;
import org.kendar.cqrses.repositories.SagaStore;
import org.kendar.cqrses.saga.SagaManager;
import org.kendar.cqrses.scheduler.Sleeper;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.kendar.cqrses.utils.ReflectionUtils;
import org.kendar.cqrses.utils.UUIDGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProcessingGroup {
    private final ConcurrentLinkedQueue<LaneWork> queue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private String name;
    private int segment;
    private final Bus bus;
    private final MessageSerializer serializer;
    private final boolean commandSide;
    private final DlqStore dlqStore;
    private final Map<Class<?>, List<Bus.Registration>> consumer;
    private final Bus.ProcessingGroupPolicyConfig policy;
    private Thread thread;

    public ProcessingGroup(String name, Bus bus, MessageSerializer serializer, boolean commandSide, DlqStore dlqStore,
                           Map<Class<?>, List<Bus.Registration>> consumer, Bus.ProcessingGroupPolicyConfig policy) {
        this.name = name;
        this.bus = bus;
        this.serializer = serializer;
        this.commandSide = commandSide;
        this.dlqStore = dlqStore;
        this.consumer = consumer;
        this.policy = policy;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getSegment() {
        return segment;
    }

    public void setSegment(int segment) {
        this.segment = segment;
    }

    public Thread getThread() {
        return thread;
    }

    public void setThread(Thread thread) {
        this.thread = thread;
    }

    public ConcurrentLinkedQueue<LaneWork> getQueue() {
        return queue;
    }

    public boolean isRunning() {
        return running.get();
    }

    public void setRunning(boolean running) {
        this.running.set(running);
    }

    public void start() {
        while (true) {
            if (!isRunning()) return;
            LaneWork work;
            while ((work = getQueue().poll()) != null) {
                if (!isRunning()) return;
                invokeConsumers(work.msg(), work.consumers());
            }
            Sleeper.yield();
        }
    }



    private static boolean isDeleteAfterCompletion(Object saga) {
        Saga ann = saga.getClass().getAnnotation(Saga.class);
        return ann == null || ann.deleteAfterCompletion();
    }



    private InternalMessage buildEventEnvelope(Object event, Context commandCtx) {
        Event ann = event.getClass().getAnnotation(Event.class);
        if (ann == null) {
            throw new InvalidHandlerException(
                    "Emitted payload " + event.getClass().getName() + " is not annotated with @Event");
        }
        UUID aggregateId = Bus.extractAggregateId(event);
        if (aggregateId == null) aggregateId = commandCtx.getAggregateId();
        if (aggregateId == null) {
            throw new InvalidHandlerException(
                    "Emitted event " + event.getClass().getName() +
                            " has no @AggregateIdentifier and the originating command carried no aggregateId");
        }
        Context ctx = new Context();
        ctx.setProcessingGroup(this.name);
        ctx.setAggregateId(aggregateId);
        ctx.setAggregateVersion(commandCtx.getAggregateVersion());
        ctx.setType(event.getClass().getSimpleName());
        ctx.setVersion(ann.version());
        ctx.setTraceId(commandCtx == null ? UUIDGenerator.newUuid() : commandCtx.getTraceId());
        ctx.setTimestamp(Instant.now());
        InternalMessage msg = new InternalMessage();
        msg.setContext(ctx);
        msg.setEvent(true);
        msg.setPayload(serializer.serialize(event));
        return msg;
    }

    /**
     * Stamp each emitted event with a Context, append them in one batch per
     * aggregate so the store's per-aggregate lock assigns a monotonic version,
     * then publish on the event bus with the assigned version. After a successful
     * append, fires the {@code @Aggregate(snapshotEvery = N)} trigger for the
     * handling aggregate when this batch crossed an N-events boundary.
     */
    private void persistAndPublishEmitted(List<Object> emitted, Context commandCtx, Object target) {
        var eventStore = GlobalRegistry.get(EventStore.class);
        var eventBus = GlobalRegistry.get(EventBus.class);
        if (eventStore == null) {
            throw new IllegalStateException(
                    "No EventStore registered in GlobalRegistry; cannot persist emitted events");
        }
        // buildEventEnvelope carries the command's aggregateVersion onto each
        // emitted event, so an explicit send(command, expectedVersion) is enforced
        // by the store's optimistic-concurrency check instead of being silently
        // downgraded to "assign next" (-1).
        var envelopes = new ArrayList<InternalMessage>(emitted.size());
        var byEvent = new LinkedHashMap<Object, InternalMessage>();
        for (Object event : emitted) {
            var envelope = buildEventEnvelope(event, commandCtx);
            envelopes.add(envelope);
            byEvent.put(event, envelope);
        }
        long appendStart = System.nanoTime();
        eventStore.appendEvents(envelopes);
        Observability.get().onEventsAppended(envelopes.size(), System.nanoTime() - appendStart);
        maybeSnapshot(eventStore, envelopes, commandCtx, target);
        if (eventBus == null) return;
        long publishStart = System.nanoTime();
        for (var entry : byEvent.entrySet()) {
            long assignedVersion = entry.getValue().getContext().getAggregateVersion();
            eventBus.send(entry.getKey(), (int) assignedVersion);
        }
        if (TraceRecorder.active()) {
            TraceRecorder.stage("publish", System.nanoTime() - publishStart, byEvent.size());
        }
    }

    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, java.lang.reflect.Method>
            snapshotGetterCache = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Automatic threshold snapshotting ({@code @Aggregate(snapshotEvery = N)}).
     * Fires when this command's batch crossed a multiple-of-N boundary in the
     * handling aggregate's stream — boundary-crossing (not modulo on the last
     * version) so a multi-event batch that jumps past a boundary still triggers.
     * Best-effort by contract (CROSS_CUTTING.md §5): a snapshot failure is logged and
     * never fails the command. The payload comes from the aggregate's
     * {@code getSnapshot()} (validated at registration); the aggregate-version
     * stamp is the batch's last assigned version, which exactly matches the
     * in-memory state being snapshotted — no re-read race with later commands.
     */
    private void maybeSnapshot(EventStore eventStore, List<InternalMessage> envelopes,
                               Context commandCtx, Object target) {
        if (target == null) return;
        var ann = target.getClass().getAnnotation(org.kendar.cqrses.annotations.Aggregate.class);
        if (ann == null || ann.snapshotEvery() <= 0) return;
        UUID aggregateId = commandCtx.getAggregateId();
        if (aggregateId == null) return;
        // Only this aggregate's envelopes count: a handler may emit events that
        // carry another aggregate's id, and those don't advance this stream.
        long first = Long.MAX_VALUE;
        long last = -1;
        for (var envelope : envelopes) {
            var ctx = envelope.getContext();
            if (!aggregateId.equals(ctx.getAggregateId())) continue;
            first = Math.min(first, ctx.getAggregateVersion());
            last = Math.max(last, ctx.getAggregateVersion());
        }
        if (last < 0) return;
        int n = ann.snapshotEvery();
        // Versions are 0-based: event count before the batch = first, after = last+1.
        if ((last + 1) / n == first / n) return; // no boundary crossed
        try {
            var getter = snapshotGetterCache.computeIfAbsent(target.getClass(), t -> {
                try {
                    return t.getMethod("getSnapshot");
                } catch (NoSuchMethodException e) {
                    throw new InvalidHandlerException("Aggregate " + t.getName()
                            + " declares snapshotEvery=" + n + " but has no getSnapshot() method");
                }
            });
            Object payload = getter.invoke(target);
            if (payload == null) return;
            eventStore.storeSnapshot(aggregateId, payload, ann.version(), last);
        } catch (Exception e) {
            LOGGER.warn("auto-snapshot for aggregate {} ({}) failed; continuing without it",
                    aggregateId, target.getClass().getSimpleName(), e);
        }
    }

    /**
     * Command-side dispatch: open a per-command event buffer via
     * {@link EventApplyer#begin()}, invoke the handler, and on success persist the
     * emitted events to the {@link EventStore} and publish each onto the
     * {@link EventBus}. On throw the buffer is discarded and the exception
     * propagates — store + bus are never touched.
     *
     * @return the {@code @CommandHandler} method's return value ({@code null} for
     * {@code void} handlers) — surfaced to the sender by the synchronous path.
     */
    private Object dispatchCommand(Bus.Registration consumer, Object target, Object message, Context ctx) {
        EventApplyer.begin();
        Object result = null;
        boolean success = false;
        long t0 = System.nanoTime();
        try {
            result = consumer.method().apply(target, message, ctx);
            success = true;
        } finally {
            // Record handler time only (before append) so onCommandHandled and
            // onEventsAppended stay disjoint and append cost isn't double-counted.
            Observability.get().onCommandHandled(policy.processingGroup(), ctx.getType(),
                    System.nanoTime() - t0, success);
            if (TraceRecorder.active()) {
                TraceRecorder.stage("handler", System.nanoTime() - t0, success ? 1 : 0);
            }
            var emitted = EventApplyer.drain();
            if (success && !emitted.isEmpty()) {
                persistAndPublishEmitted(emitted, ctx, target);
            }
        }
        return result;
    }

    /**
     * Event-side dispatch: invoke the handler, then (only for saga targets)
     * persist the saga back to the {@link SagaStore} so any state mutated inside
     * the handler — including correlation values set on {@code @SagaStart} — is
     * visible to the next event.
     */
    private void dispatchEvent(Bus.Registration consumer, Object target, Object message, Context ctx) {
        var targetType = GlobalRegistry.getTargetType(consumer.handlerClass());
        boolean isSaga = targetType == TargetType.SAGA;
        int seg = ctx.getAggregateId() == null ? 0 : SegmentCalculator.calculateSegment(ctx.getAggregateId());
        boolean ok = false;
        long t0 = System.nanoTime();
        try {
            consumer.method().apply(target, message, ctx);
            ok = true;
            if (!isSaga) return;
            var sagaStore = GlobalRegistry.get(SagaStore.class);
            if (sagaStore == null) return;
            if (SagaManager.isSagaEnded() && isDeleteAfterCompletion(target)) {
                sagaStore.deleteSaga(target);
            } else {
                sagaStore.storeSaga(target);
            }
        } finally {
            long elapsed = System.nanoTime() - t0;
            if (isSaga) {
                Observability.get().onSagaDispatched(policy.processingGroup(), seg, ctx.getType(), elapsed, ok);
                SagaManager.clear();
            } else {
                Observability.get().onEventDispatched(policy.processingGroup(), seg, ctx.getType(), elapsed, ok);
            }
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingGroup.class);
    /**
     * Invoke every consumer registered for this message in the group, in
     * registration order. Throws on the first handler failure (no swallowing) so
     * the caller decides whether to retry-and-block or log-and-skip. A null
     * target is legitimate only for sagas with no correlation match — every other
     * null is a misconfiguration and surfaces as {@link InvalidHandlerException}.
     */
    public void invokeConsumers( InternalMessage toSend, List<Bus.Registration> consumers) {
        var enqueuePolicy = policy.enqueuePolicy();
        var messageType = bus.getMessageClass(toSend.getContext().getType());
        String sequenceId = null;
        try {
            var message = serializer.deserialize(toSend.getPayload(), messageType);
            sequenceId = policy.sequencePolicy().getSequenceId(message);

            var hasBlockedItems = dlqStore.hasBlockedItems(sequenceId);
            if(hasBlockedItems){
                LOGGER.warn("processing group '{}' routing {} {} to DLQ: aggregate/sequence '{}' "
                                + "blocked by a prior failure (head-of-line)",
                        policy.processingGroup(), commandSide ? "command" : "event",
                        toSend.getContext().getType(), sequenceId);
                transactionStart();
                addToDlq(sequenceId, toSend, null);
                transactionEnd();
                return;
            }
            //HERE SHOULD START TRANSACTION
            transactionStart();
            for (var consumer : consumers) {
                var target = bus.findTarget(message, consumer);
                if (target == null) {
                    var targetType = GlobalRegistry.getTargetType(consumer.handlerClass());
                    if (targetType == TargetType.SAGA) continue;
                    throw new InvalidHandlerException("Cannot find stored item for " + consumer.handlerClass().getName());
                }

                try {
                    if (commandSide) {
                        dispatchCommand(consumer, target, message, toSend.getContext());
                        // A command has a single handler; stop after it but fall through to
                        // transactionEnd() so the boundary commits its bound connection. An
                        // early return here would skip the commit and leak an uncommitted,
                        // still-bound connection (the store no longer commits it for us).
                        break;
                    } else {
                        dispatchEvent(consumer, target, message, toSend.getContext());

                    }
                }catch (Exception e){
                    var result = enqueuePolicy.shouldEnqueue(toSend, e);
                    if(result.shouldIgnore()){
                        // The IGNORE policy silently drops a failed handler. Log it so a
                        // dropped message is observable rather than vanishing (synchronous
                        // commands no longer reach here — they propagate via invokeCommandSync).
                        LOGGER.warn("processing group '{}' IGNORING {} {}: {}",
                                policy.processingGroup(), commandSide ? "command" : "event",
                                toSend.getContext().getType(), e.toString());
                        continue;
                    }
                    if(result.shouldEvict()){
                        dlqStore.evictFirst(sequenceId);
                        break;
                    }
                    // Mark a handler failure so the outer catch can unwrap it to the
                    // real cause for the DLQ. A typed marker (grill item 5) replaces the
                    // old new Exception("WRAP", e) sentinel: it can't be confused with a
                    // handler that legitimately throws a message of "WRAP", and the outer
                    // branch no longer NPEs on a null getMessage() (e.g. a bare NPE).
                    throw new HandlerInvocationException(e);
                }
            }
            transactionEnd();
        }catch (Exception e){
            transactionRollback();
            transactionStart();
            // Unwrap a marked handler failure to its real cause up front, so the policy
            // and the DLQ both see the actual exception (not the marker) consistently.
            Exception ex = (e instanceof HandlerInvocationException) ? (Exception) e.getCause() : e;
            var result = enqueuePolicy.shouldEnqueue(toSend, ex);
            if(result.shouldIgnore())return;
            if(result.shouldEvict() && sequenceId!=null){
                dlqStore.evictFirst(sequenceId);
            }
            if(result.shouldEnqueue() && sequenceId!=null){
                addToDlq(sequenceId, toSend,ex);
            }
            LOGGER.error("Error processing message in processing group", e);
            transactionEnd();
        }
    }

    protected void transactionRollback() {
    }

    protected void transactionStart() {
        
    }

    protected void transactionEnd() {
        
    }

    private void addToDlq(String sequenceId, InternalMessage toSend,Throwable e) {
        DlqItem item = new DlqItem();
        item.setId(UUID.randomUUID());
        item.setSequenceId(sequenceId);
        item.setProcessingGroup(policy.processingGroup());
        item.setAggregateId(toSend.getContext().getAggregateId());
        item.setEventType(toSend.getContext().getType());
        item.setStatus(DlqItemStatus.PENDING);
        item.setRetryCount(0);
        item.setFailedAt(Instant.now());
        // Capture the full failure Context so a DlqManager can rebuild the
        // InternalMessage and re-invoke the handler on retry (traceId, metadata,
        // aggregateVersion preserved, not just the flattened fields above).
        item.setProcessingContext(toSend.getContext());
        // e is null on the "already blocked" path: the message itself did not
        // fail, it is being routed to the DLQ because the processing group (or
        // aggregate) is blocked by a prior failure. There is no exception to record.
        if (e != null) {
            item.setErrorMessage(e.getMessage());
            item.setErrorClass(e.getClass().getName());
            item.setStackTrace(ReflectionUtils.stackTraceOf(e));
        } else {
            item.setErrorMessage("Blocked by processing group " + policy.processingGroup()
                    + " due to a prior failure");
        }
        item.setSerializedEvent(toSend.getPayload());
        dlqStore.addItem(item, sequenceId);
        Observability.get().onDlqEnqueued(policy.processingGroup(), toSend.getContext().getType());
    }

    /** Reload-and-retry budget for a synchronous command that loses a version race. */
    private static final int MAX_COMMAND_OCC_RETRIES = 8;
    private static final long COMMAND_OCC_BACKOFF_MS = 5L;

    /**
     * Synchronous command dispatch for {@link ProcessingGroupsManager#sendSync}.
     * Unlike {@link #invokeConsumers}, a synchronous command does <b>not</b> absorb
     * failures into the DLQ or swallow them under the group's IGNORE policy — it
     * propagates to its sender (the {@code sendSync} contract), so an HTTP caller
     * learns the command did not land instead of receiving a false ack.
     *
     * <p>An {@link OptimisticConcurrencyException} — two nodes appending to the same
     * aggregate concurrently in the cluster (caught by {@code UNIQUE(aggregate_id,
     * sequence)}), or a stale expected version — is retried up to
     * {@value #MAX_COMMAND_OCC_RETRIES} times. Each attempt re-resolves the target,
     * which reloads the aggregate and recomputes the next version against the winner's
     * just-committed event, so the loser normally lands on the next try. Only if it
     * still conflicts after the budget does the violation propagate.
     *
     * @return the {@code @CommandHandler}'s return value ({@code null} for
     * {@code void} handlers), surfaced to the sender via {@code sendSync}.
     */
    public Object invokeCommandSync(InternalMessage toSend, List<Bus.Registration> consumers) {
        var messageType = bus.getMessageClass(toSend.getContext().getType());
        var message = serializer.deserialize(toSend.getPayload(), messageType);
        var ctx = toSend.getContext();
        for (var consumer : consumers) {
            int attempt = 0;
            while (true) {
                var target = bus.findTarget(message, consumer); // reloads the aggregate each attempt
                if (target == null) {
                    if (GlobalRegistry.getTargetType(consumer.handlerClass()) == TargetType.SAGA) break;
                    throw new InvalidHandlerException(
                            "Cannot find stored item for " + consumer.handlerClass().getName());
                }
                try {
                    return dispatchCommand(consumer, target, message, ctx);
                } catch (OptimisticConcurrencyException occ) {
                    if (++attempt > MAX_COMMAND_OCC_RETRIES) {
                        LOGGER.warn("command {} on aggregate {} still conflicting after {} retries; propagating",
                                ctx.getType(), ctx.getAggregateId(), MAX_COMMAND_OCC_RETRIES);
                        throw occ;
                    }
                    TraceRecorder.stage("occ.retry", 0, attempt);
                    Sleeper.sleep(COMMAND_OCC_BACKOFF_MS);
                }
            }
        }
        return null;
    }

    /**
     * Internal marker for "a handler invocation threw": carries the original cause so
     * {@link #invokeConsumers} can route it to the DLQ. Replaces the old
     * {@code new Exception("WRAP", e)} string sentinel (grill item 5).
     */
    private static final class HandlerInvocationException extends Exception {
        HandlerInvocationException(Throwable cause) {
            super(cause);
        }
    }
}
