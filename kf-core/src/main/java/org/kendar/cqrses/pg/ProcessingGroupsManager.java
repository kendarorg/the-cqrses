package org.kendar.cqrses.pg;

import org.kendar.cqrses.bus.Bus;
import org.kendar.cqrses.bus.CommandBus;
import org.kendar.cqrses.bus.EventBus;
import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.di.TargetType;
import org.kendar.cqrses.dlq.DlqStore;
import org.kendar.cqrses.exceptions.InvalidHandlerException;
import org.kendar.cqrses.exceptions.InvalidRegistrationException;
import org.kendar.cqrses.serialization.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ProcessingGroupsManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingGroupsManager.class);
    protected final Bus bus;
    protected final MessageSerializer serializer;
    protected final boolean commandSide;
    protected final DlqStore dlqStore;
    private final Map<String, ProcessingGroup[]> bySegment = new HashMap<>();
    private final Map<String, SagaResolver> resolvers = new HashMap<>();
    private Map<String, Map<Class<?>, List<Bus.Registration>>> consumersByGroup;
    /**
     * Cluster pull mode: when set (only ever on the event-side manager), {@code start}
     * builds the {@code ProcessingGroup}/{@code SagaResolver} objects but spawns no
     * lane or resolver threads, and {@code send} stops pushing to lanes — the
     * {@code SegmentProcessor} owns the threads and feeds the lanes from the store.
     * The command side is untouched. See {@code docs/tricks.md}.
     */
    private boolean pullMode = false;

    public ProcessingGroupsManager(Bus bus, MessageSerializer serializer, DlqStore dlqStore) {
        this.bus = bus;
        this.serializer = serializer;
        this.commandSide = bus instanceof CommandBus;
        this.dlqStore = dlqStore;
    }

    /** Enable cluster pull mode. MUST be called before {@link #start} (setup phase). */
    public void setPullMode(boolean pullMode) {
        this.pullMode = pullMode;
    }

    public boolean isPullMode() {
        return pullMode;
    }

    public void start(Map<String, Map<Class<?>, List<Bus.Registration>>> consumers) {
        this.consumersByGroup = consumers;
        // Restart idempotency: drop any previously-built lanes/resolvers.
        bySegment.clear();
        resolvers.clear();

        consumers.forEach((group, consumer) -> {
            var policy = bus.getProcessingGroupPolicy(group);

            // Classify (event side only; command side is inherently aggregate-only).
            boolean sagaGroup = false;
            if (!commandSide) {
                boolean hasSaga = false;
                boolean hasProjection = false;
                for (var regs : consumer.values()) {
                    for (var reg : regs) {
                        var tt = GlobalRegistry.getTargetType(reg.handlerClass());
                        if (tt == TargetType.SAGA) hasSaga = true;
                        else if (tt == TargetType.PROJECTION) hasProjection = true;
                    }
                }
                if (hasSaga && hasProjection) {
                    throw new InvalidRegistrationException("Event-bus processing group '" + group
                            + "' mixes SAGA and PROJECTION handlers; a group must be homogeneous (all-saga or all-projection).");
                }
                sagaGroup = hasSaga;
            }

            // Projection concurrency contract (grill item 7), uniform across backends:
            // a @Projection singleton MUST be concurrency-safe — ordering is guaranteed
            // per-aggregate only. Push mode invokes the singleton on up to SEGMENTS lane
            // threads concurrently; pull mode (SegmentProcessor) exposes the same knob via
            // kf.cluster.dispatch-concurrency, with =1 the serial opt-out on either side.
            ProcessingGroup[] lanes = new ProcessingGroup[SegmentCalculator.getSegments()];
            for (int i = 0; i < SegmentCalculator.getSegments(); i++) {
                var pg = createProcessingGroup(group, consumer, policy);
                pg.setSegment(i);
                lanes[i] = pg;
            }
            bySegment.put(group, lanes);
            // Pull mode (cluster, event side): build the lanes but DON'T spawn their
            // threads — the SegmentProcessor drives them from the store tail.
            if (!pullMode) {
                for (var pg : lanes) {
                    pg.setThread(new Thread(pg::start));
                    pg.getThread().start();
                }
            }

            if (sagaGroup) {
                // Thread-less create-PG, for @SagaStart inline creation on the resolver thread.
                var createPg = createProcessingGroup(group, consumer, policy);
                var resolver = new SagaResolver(group, (EventBus) bus, serializer, consumer, lanes, createPg);
                resolvers.put(group, resolver);
                // Pull mode: the SegmentProcessor's saga workers drive resolve() instead.
                if (!pullMode) {
                    resolver.setThread(new Thread(resolver::start));
                    resolver.getThread().start();
                }
            }
        });
    }

    protected ProcessingGroup createProcessingGroup(String group, Map<Class<?>, List<Bus.Registration>> consumer, Bus.ProcessingGroupPolicyConfig policy) {
        return new ProcessingGroup(group, bus, serializer, commandSide, dlqStore, consumer, policy);
    }

    public void send(Set<String> pgs, InternalMessage msg) {
        // Pull mode (cluster, event side): events reach lanes only via the
        // SegmentProcessor poller. The local push is a no-op so events aren't
        // dispatched twice. The append-to-store during command handling is
        // unaffected (it happens before this publish). The nudge wakes this
        // node's pull pumps so locally-appended events dispatch without
        // waiting for the backstop poll (deferred until commit when a
        // transaction boundary is bound — see PumpNudger).
        if (pullMode) {
            PumpNudger.notifyAppend();
            return;
        }
        for (var group : pgs) {
            SagaResolver resolver = resolvers.get(group);
            if (resolver != null) {
                if (resolver.isRunning()) resolver.getQueue().add(msg);
                continue;
            }
            ProcessingGroup[] lanes = bySegment.get(group);
            if (lanes == null) continue;
            UUID key = msg.getContext().getAggregateId();
            int segment;
            if (key == null) {
                if (!commandSide) {
                    throw new InvalidHandlerException("Event '" + msg.getContext().getType()
                            + "' has no aggregateId; cannot route to a lane in projection group '" + group + "'");
                }
                segment = 0; // command-side interceptors may carry no aggregateId
            } else {
                segment = SegmentCalculator.calculateSegment(key);
            }
            ProcessingGroup lane = lanes[segment];
            if (lane.isRunning()) {
                var messageType = bus.getMessageClass(msg.getContext().getType());
                var list = consumersByGroup.get(group).get(messageType);
                lane.getQueue().add(new LaneWork(msg, list));
            }
        }
    }

    /**
     * Synchronous dispatch (command-bus {@code sendSync}). A synchronous send is
     * Axon's command-bus model, not the streaming-processor one: it invokes the
     * handlers on the caller's thread and lets any exception <b>propagate back to
     * the sender</b> — there is no worker thread to retry on, and blocking the
     * caller forever would be wrong. The async {@link #send} path is where the
     * retry/head-of-line-block semantics live.
     *
     * @return the command handler's return value (the last non-null one when the
     * command type spans several groups, e.g. interceptor + aggregate groups);
     * {@code null} for {@code void} handlers and on the event side.
     */
    public Object sendSync(Set<String> pgs, Map<String, Map<Class<?>, List<Bus.Registration>>> consumers,
                           InternalMessage toSend) {
        Object result = null;
        for (var pg : pgs) {
            ProcessingGroup[] lanes = bySegment.get(pg);
            if (lanes == null) continue;
            ProcessingGroup pgg = lanes[0];
            if (!pgg.isRunning()) continue;
            var foundedConsumers = consumers.get(pg);
            var messageType = bus.getMessageClass(toSend.getContext().getType());
            var singleConsumer = foundedConsumers.get(messageType);
            // A synchronous command propagates failures to its sender and retries an
            // optimistic-concurrency loss with a fresh aggregate load (the cluster's
            // two-nodes-one-aggregate race), rather than swallowing it under the group's
            // IGNORE policy and falsely acking. The event/async path keeps DLQ semantics.
            if (commandSide) {
                Object r = pgg.invokeCommandSync(toSend, singleConsumer);
                if (r != null) result = r;
            } else {
                pgg.invokeConsumers(toSend, singleConsumer);
            }
        }
        return result;
    }

    /**
     * Re-deliver an operator-redispatched dead letter to the live event-side worker.
     * <p>
     * In <b>push</b> mode this is exactly {@link #send}: the message is enqueued onto
     * its lane (or the saga resolver) and the running lane thread picks it up. In
     * <b>pull</b> mode there are no lane threads and {@link #send} is a no-op, so the
     * message is dispatched <b>synchronously through the live group</b> — the same
     * code path the {@code SegmentProcessor} pump uses ({@link #dispatchProjection}
     * for a projection group, the {@link SagaResolver} for a saga group). This is
     * what makes DLQ {@code redispatch} work under the cluster, not just single-node
     * push. A re-failure is dead-lettered again through the group's real policy
     * inside {@code invokeConsumers}.
     */
    public void redeliver(Set<String> pgs, InternalMessage msg) {
        if (!pullMode) {
            send(pgs, msg);
            return;
        }
        for (var group : pgs) {
            SagaResolver resolver = resolvers.get(group);
            if (resolver != null) {
                // The saga's owning segment isn't known here; the resolver's own
                // segment gate fires only the matching slot, so sweep them all.
                for (int k = 0; k < SegmentCalculator.getSegments(); k++) {
                    resolver.resolveForSegment(msg, k);
                }
                continue;
            }
            if (!bySegment.containsKey(group)) continue;
            UUID agg = msg.getContext().getAggregateId();
            int seg = (agg == null) ? 0 : SegmentCalculator.calculateSegment(agg);
            dispatchProjection(group, seg, msg);
        }
    }

    /** True iff this handler owns (dispatches) the given processing group. */
    public boolean handlesGroup(String group) {
        return bySegment.containsKey(group) || resolvers.containsKey(group);
    }

    /** The processing groups this handler dispatches, for DLQ-retry enumeration. */
    public Set<String> groups() {
        Set<String> union = new HashSet<>(bySegment.keySet());
        union.addAll(resolvers.keySet());
        return Set.copyOf(union);
    }

    // ---- cluster pull-mode surface (driven by SegmentProcessor) ----

    /** Event groups whose handlers are sagas (have a {@link SagaResolver}). */
    public Set<String> sagaGroups() {
        return Set.copyOf(resolvers.keySet());
    }

    /** Event groups whose handlers are projections (lanes built, not saga). */
    public Set<String> projectionGroups() {
        Set<String> s = new HashSet<>(bySegment.keySet());
        s.removeAll(resolvers.keySet());
        return Set.copyOf(s);
    }

    /** The lane object for {@code (group, segment)} (no thread in pull mode). */
    public ProcessingGroup laneFor(String group, int segment) {
        ProcessingGroup[] lanes = bySegment.get(group);
        return lanes == null ? null : lanes[segment];
    }

    /** The saga resolver for {@code group}, or null if not a saga group. */
    public SagaResolver resolver(String group) {
        return resolvers.get(group);
    }

    /**
     * Simple-name set of every event type {@code group} subscribes to — the filter
     * for a saga worker's per-source tail reads ({@code loadSegmentTypeTail}).
     */
    public Set<String> subscribedEventTypes(String group) {
        var byType = consumersByGroup.get(group);
        if (byType == null) return Set.of();
        Set<String> types = new HashSet<>();
        for (Class<?> c : byType.keySet()) {
            types.add(c.getSimpleName());
        }
        return types;
    }

    /**
     * Synchronously dispatch one event to a projection {@code (group, segment)}
     * lane (cluster pull pump). No-op if the group has no consumer for the event
     * type. Errors are handled inside {@code invokeConsumers} (DLQ / log), so this
     * does not throw — the pump always advances its checkpoint (at-least-once).
     */
    public void dispatchProjection(String group, int segment, InternalMessage msg) {
        ProcessingGroup lane = laneFor(group, segment);
        if (lane == null) {
            LOGGER.debug("dispatchProjection group={} seg={} type={} NO-OP: no lane",
                    group, segment, msg.getContext().getType());
            return;
        }
        var messageType = bus.getMessageClass(msg.getContext().getType());
        if (messageType == null) {
            LOGGER.debug("dispatchProjection group={} seg={} type={} NO-OP: getMessageClass returned null",
                    group, segment, msg.getContext().getType());
            return;
        }
        var byType = consumersByGroup.get(group);
        if (byType == null) {
            LOGGER.debug("dispatchProjection group={} seg={} type={} NO-OP: no consumers for group",
                    group, segment, msg.getContext().getType());
            return;
        }
        var list = byType.get(messageType);
        if (list == null || list.isEmpty()) {
            // Normal filter: each group's pump reads every event in its owned segments
            // and skips the types it does not subscribe to.
            LOGGER.debug("dispatchProjection group={} seg={} type={} NO-OP: group does not subscribe",
                    group, segment, msg.getContext().getType());
            return;
        }
        LOGGER.debug("dispatchProjection group={} seg={} type={} -> {} consumer(s)",
                group, segment, msg.getContext().getType(), list.size());
        lane.invokeConsumers(msg, list);
    }

    public void stop() {
        for (var lanes : bySegment.values()) {
            for (var pg : lanes) {
                pg.setRunning(false);
            }
        }
        for (var resolver : resolvers.values()) {
            resolver.setRunning(false);
        }
    }

    public void clear() {
        for (var lanes : bySegment.values()) {
            for (var pg : lanes) {
                pg.getQueue().clear();
            }
        }
        for (var resolver : resolvers.values()) {
            resolver.getQueue().clear();
        }
    }
}
