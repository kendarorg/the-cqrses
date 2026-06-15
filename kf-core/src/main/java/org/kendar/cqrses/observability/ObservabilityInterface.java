package org.kendar.cqrses.observability;

/**
 * Framework-internal observability seam. Every hot path in the dispatch
 * machinery calls one of these semantic methods once it has measured its own
 * elapsed time; an implementation turns the call into a timer / counter.
 *
 * <p><b>kf-core ships only the {@link NullObservability} no-op.</b> The intent
 * is that {@code kf-core} stays free of any metrics dependency — a concrete
 * implementation (e.g. the {@code MicrometerTimers} in {@code kf-spring}) is
 * installed at bootstrap through {@link Observability#set(ObservabilityInterface)}.
 *
 * <p>Methods are <b>semantic</b> (one per measured operation) rather than a
 * generic {@code time(name, ...)} so the implementation maps each call 1:1 to a
 * meter name without string juggling, and call sites read as documentation.
 * Durations are passed as elapsed nanoseconds measured with
 * {@link System#nanoTime()} at the call site; counters carry no duration.
 *
 * <p>Implementations MUST be cheap and MUST NOT throw — they run on the dispatch
 * threads. A throwing implementation would corrupt the very path it is meant to
 * observe.
 */
public interface ObservabilityInterface {

    /**
     * A command handler ran. {@code group} is the processing group, {@code
     * commandType} the command's logical type, {@code nanos} the handler
     * wall-clock, {@code ok} whether it returned without throwing.
     */
    void onCommandHandled(String group, String commandType, long nanos, boolean ok);

    /**
     * An aggregate was rehydrated from its event stream (optionally short-cut by
     * a snapshot). {@code eventsReplayed} is how many events were folded.
     */
    void onAggregateRehydrated(String aggregateType, int eventsReplayed, long nanos);

    /**
     * A batch of emitted events was appended to the event store. {@code count}
     * is the batch size.
     */
    void onEventsAppended(int count, long nanos);

    /**
     * A projection event handler ran for one event in {@code segment}.
     */
    void onEventDispatched(String group, int segment, String eventType, long nanos, boolean ok);

    /**
     * A saga event handler ran for one event in {@code segment}.
     */
    void onSagaDispatched(String group, int segment, String eventType, long nanos, boolean ok);

    /**
     * The pull-mode worker read a tail batch from the event store. {@code
     * eventsRead} is the number of events returned by the read.
     */
    void onSegmentTailRead(String group, int eventsRead, long nanos);

    /**
     * A processing-group checkpoint high-water-mark was committed for one
     * segment.
     */
    void onCheckpointSaved(String group, int segment);

    /**
     * An event was dead-lettered for {@code group}.
     */
    void onDlqEnqueued(String group, String eventType);

    /**
     * One SQL statement executed through the {@code Db} wrapper. {@code category}
     * is a low-cardinality {@code verb:table} label (e.g. {@code select:event_entry},
     * {@code insert:dlq_item}) — deliberately <b>not</b> per-query, so the
     * resulting meter cardinality stays bounded. {@code ok} is whether the
     * statement returned without throwing. This separates raw database time from
     * handler time when chasing a bottleneck.
     */
    void onSqlExecuted(String category, long nanos, boolean ok);

    /**
     * One phase of an event-store append completed. {@code phase} is one of the
     * low-cardinality constants in {@link AppendPhase} ({@code conn}, {@code lock},
     * {@code currentMax}, {@code insert}, {@code counter}, {@code commit},
     * {@code boundaryCommit}). Splits the append wall-clock already reported by
     * {@link #onEventsAppended(int, long)} into its serialisation points —
     * connection acquisition, segment-counter lock wait, OCC read, row inserts
     * and the commit/fsync — so the saturating stage is directly visible.
     *
     * <p>Default no-op so pre-existing implementations keep compiling.
     */
    default void onAppendPhase(String phase, long nanos) {
    }

    /**
     * An append entered ({@code delta = +1}) or left ({@code delta = -1}) the
     * per-aggregate append section for {@code segment}. The running sum is the
     * number of in-flight appends per segment — pinned at 1 it means the
     * segment-counter lock fully serialises that segment.
     */
    default void onAppendInFlight(int segment, int delta) {
    }

    /**
     * One group-commit append transaction wrote {@code requests} coalesced append
     * requests ({@code events} event rows) under a single commit/fsync. A mean
     * near 1 means appends are uncontended (the coalescer degenerates to the old
     * one-commit-per-command shape); a growing mean is the group commit actually
     * amortising the fsync under load.
     */
    default void onAppendBatch(int requests, int events) {
    }

    /**
     * A pump nudge fired. {@code deferred} is {@code true} when the nudge waited
     * for the surrounding transaction boundary to commit before waking the pumps.
     */
    default void onPumpNudge(boolean deferred) {
    }

    /**
     * Sampled pull-pump backlog for one {@code (group, segment)}: head of the
     * segment counter minus the committed checkpoint, in events. A growing lag
     * means the event side, not the command side, is the laggard.
     */
    default void onPumpLag(String group, int segment, long lagEvents) {
    }

    /**
     * A command was forwarded to the segment-owning node {@code targetNode}.
     * {@code sync} distinguishes Wait ({@code sendSync}) from Ack (async
     * {@code send}); {@code nanos} is the full round-trip on the sender;
     * {@code ok} is whether a successful response came back (a remote handler
     * exception or a timeout counts as {@code false}).
     */
    default void onCommandForwarded(String commandType, String targetNode, boolean sync, long nanos, boolean ok) {
    }

    /**
     * A command that could have been forwarded fell back to local execution.
     * {@code reason} is low-cardinality: {@code disabled}, {@code no-route},
     * {@code self}, {@code connect-failed}, {@code ack-timeout}.
     */
    default void onForwardFallback(String commandType, String reason) {
    }

    /**
     * This node executed a command received from a peer over the forwarding
     * channel. {@code nanos} is the local pipeline time, {@code ok} whether the
     * handler completed without throwing.
     */
    default void onForwardServed(String commandType, boolean sync, long nanos, boolean ok) {
    }

    /**
     * The cluster routing table was refreshed from the database.
     * {@code assignments} is the number of segment→owner rows read,
     * {@code forwardableNodes} the number of nodes advertising a forward port.
     */
    default void onRoutingRefreshed(int assignments, int forwardableNodes, long nanos) {
    }
}
