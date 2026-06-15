package org.kendar.pfm.metrics;

import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Delegating {@link EventStorageEngine} that times the three hot paths whose kf equivalents the
 * comparison report queries, feeding {@link KfMeters}:
 * <ul>
 *   <li>{@link #appendEvents(List)} → {@code kf.events.append} (count = batch size).</li>
 *   <li>{@link #readEvents(String, long)} → {@code kf.aggregate.rehydrate} (the per-aggregate stream
 *       read on command-side load; events-replayed count is best-effort 0 since the stream is lazy).</li>
 *   <li>{@link #readEvents(TrackingToken, boolean)} → {@code kf.segment.tail.read} (the streaming
 *       tail read the pooled processors pull from; events-read count best-effort 0).</li>
 * </ul>
 * Every other method is a pure delegate. {@link KfMeters} is resolved lazily (via a {@link Supplier})
 * so this decorator can be installed by a {@code BeanPostProcessor} before the meters bean exists.
 * Best-effort: a null sink simply skips recording.
 */
public class TimingEventStorageEngine implements EventStorageEngine {

    private final EventStorageEngine delegate;
    private final Supplier<KfMeters> meters;

    public TimingEventStorageEngine(EventStorageEngine delegate, Supplier<KfMeters> meters) {
        this.delegate = delegate;
        this.meters = meters;
    }

    private KfMeters meters() {
        try {
            return meters.get();
        } catch (RuntimeException e) {
            return null;
        }
    }

    @Override
    public void appendEvents(List<? extends EventMessage<?>> events) {
        long t0 = System.nanoTime();
        delegate.appendEvents(events);
        KfMeters m = meters();
        if (m != null) {
            m.onEventsAppended(events.size(), System.nanoTime() - t0);
        }
    }

    @Override
    public DomainEventStream readEvents(String aggregateIdentifier, long firstSequenceNumber) {
        long t0 = System.nanoTime();
        DomainEventStream stream = delegate.readEvents(aggregateIdentifier, firstSequenceNumber);
        KfMeters m = meters();
        if (m != null) {
            m.onAggregateRehydrated("Account", 0, System.nanoTime() - t0);
        }
        return stream;
    }

    @Override
    public Stream<? extends TrackedEventMessage<?>> readEvents(TrackingToken trackingToken, boolean mayBlock) {
        long t0 = System.nanoTime();
        Stream<? extends TrackedEventMessage<?>> stream = delegate.readEvents(trackingToken, mayBlock);
        KfMeters m = meters();
        if (m != null) {
            m.onSegmentTailRead("stream", 0, System.nanoTime() - t0);
        }
        return stream;
    }

    @Override
    public void storeSnapshot(DomainEventMessage<?> snapshot) {
        delegate.storeSnapshot(snapshot);
    }

    @Override
    public Optional<DomainEventMessage<?>> readSnapshot(String aggregateIdentifier) {
        return delegate.readSnapshot(aggregateIdentifier);
    }

    @Override
    public Optional<Long> lastSequenceNumberFor(String aggregateIdentifier) {
        return delegate.lastSequenceNumberFor(aggregateIdentifier);
    }

    @Override
    public TrackingToken createTailToken() {
        return delegate.createTailToken();
    }

    @Override
    public TrackingToken createHeadToken() {
        return delegate.createHeadToken();
    }

    @Override
    public TrackingToken createTokenAt(Instant dateTime) {
        return delegate.createTokenAt(dateTime);
    }
}
