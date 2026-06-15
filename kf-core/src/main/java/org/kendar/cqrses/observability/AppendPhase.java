package org.kendar.cqrses.observability;

/**
 * Low-cardinality phase labels for
 * {@link ObservabilityInterface#onAppendPhase(String, long)}. Constant strings —
 * the append hot path emits one call per phase with zero allocation (a struct
 * per append would allocate even when observability is the no-op).
 */
public final class AppendPhase {

    /** Connection acquisition + autocommit toggle (≈0 when a boundary connection is bound). */
    public static final String CONN = "conn";
    /** Segment-counter upsert + {@code SELECT ... FOR UPDATE} — the per-segment lock wait. */
    public static final String LOCK = "lock";
    /** {@code MAX(sequence)} OCC read for the aggregate. */
    public static final String CURRENT_MAX = "currentMax";
    /** The per-event {@code event_entry} INSERT loop. */
    public static final String INSERT = "insert";
    /** The segment-counter advance UPDATE. */
    public static final String COUNTER = "counter";
    /** The owned-connection commit (the fsync suspect). */
    public static final String COMMIT = "commit";
    /** The async-boundary commit in {@code JdbcProcessingGroup.transactionEnd}. */
    public static final String BOUNDARY_COMMIT = "boundaryCommit";

    private AppendPhase() {
    }
}
