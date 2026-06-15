package org.kendar.cqrses.bus;

public class InternalMessage {
    private boolean isEvent;
    private byte[] payload;
    private Context context;
    private boolean isRetry = false;
    /**
     * Per-segment, gap-free tail position assigned by the event store on append
     * (see the {@code segment_seq} column / {@code segment_counter} table). Only
     * populated on messages returned by the segment tail reads
     * ({@code loadSegmentTail} / {@code loadSegmentTypeTail}); {@code -1} elsewhere.
     */
    private long segmentSeq = -1L;
    /**
     * Store-side append timestamp (epoch millis). The cross-segment merge key for
     * the saga k-way merge. Only populated on messages returned by the segment
     * tail reads; {@code 0} elsewhere.
     */
    private long createdAt = 0L;


    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public Context getContext() {
        return context;
    }

    public void setContext(Context context) {
        this.context = context;
    }

    public boolean isEvent() {
        return isEvent;
    }

    public void setEvent(boolean event) {
        isEvent = event;
    }

    public boolean isRetry() {
        return isRetry;
    }

    public void setRetry(boolean retry) {
        isRetry = retry;
    }

    public long getSegmentSeq() {
        return segmentSeq;
    }

    public void setSegmentSeq(long segmentSeq) {
        this.segmentSeq = segmentSeq;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }
}
