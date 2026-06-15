package org.kendar.cqrses.repositories;

/**
 * Durable high-water-marks for the cluster event-side pull pump. One logical row
 * per {@code (processingGroup, segment, sourceSegment)}:
 * <ul>
 *   <li>A projection group writes one row per owned segment
 *       ({@code sourceSegment == segment}).</li>
 *   <li>A saga group writes {@code SEGMENTS} rows per owned segment — one per
 *       merged source stream.</li>
 * </ul>
 * Because {@code segment_seq} is gap-free, {@code lastSeq} is a clean
 * high-water-mark — no gap tracking. A new owner of a segment resumes each cursor
 * exactly where the previous owner left off. See {@code docs/tricks.md}.
 */
public interface CheckpointStore {

    /**
     * The last {@code segment_seq} processed for this {@code (group, segment,
     * sourceSegment)} cursor, or {@code -1} if there is no checkpoint yet (start
     * from the beginning of the stream).
     */
    long load(String processingGroup, int segment, int sourceSegment);

    /**
     * Advance the high-water-mark for this cursor. Called after a batch (or single
     * event) has been successfully dispatched — the pump is checkpoint-after-process
     * (at-least-once on crash/handoff).
     *
     * <p><b>Monotonic:</b> this never moves the cursor backward — it keeps
     * {@code max(stored, lastSeq)}. During an owner-overlap window (a lagging old
     * owner and a faster new owner both writing the same row before the lease
     * clears) a stale, lower write therefore cannot regress the checkpoint and make
     * the new owner re-tail. To intentionally rewind for a rebuild, use
     * {@link #reset}.
     */
    void save(String processingGroup, int segment, int sourceSegment, long lastSeq);

    /**
     * Force the cursor to an exact value, <b>including backward</b>. The only
     * legitimate rewind: operator replay ({@link org.kendar.cqrses.pg.SegmentProcessor#replay})
     * seeds the checkpoint to {@code fromSeq - 1} so a fresh worker re-tails from
     * history. The monotonic {@link #save} would refuse such a lowering.
     */
    void reset(String processingGroup, int segment, int sourceSegment, long lastSeq);
}
