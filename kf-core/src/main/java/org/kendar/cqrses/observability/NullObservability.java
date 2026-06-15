package org.kendar.cqrses.observability;

/**
 * No-op {@link ObservabilityInterface} — the default installed in
 * {@link Observability} until something replaces it at bootstrap. Every method
 * is empty, so an uninstrumented deployment (and every {@code kf-core} /
 * {@code kf-core-db} test) pays only a single volatile read plus the
 * {@link System#nanoTime()} bracketing at each call site, with no metrics
 * dependency dragged into {@code kf-core}.
 */
public final class NullObservability implements ObservabilityInterface {

    @Override
    public void onCommandHandled(String group, String commandType, long nanos, boolean ok) {
    }

    @Override
    public void onAggregateRehydrated(String aggregateType, int eventsReplayed, long nanos) {
    }

    @Override
    public void onEventsAppended(int count, long nanos) {
    }

    @Override
    public void onEventDispatched(String group, int segment, String eventType, long nanos, boolean ok) {
    }

    @Override
    public void onSagaDispatched(String group, int segment, String eventType, long nanos, boolean ok) {
    }

    @Override
    public void onSegmentTailRead(String group, int eventsRead, long nanos) {
    }

    @Override
    public void onCheckpointSaved(String group, int segment) {
    }

    @Override
    public void onDlqEnqueued(String group, String eventType) {
    }

    @Override
    public void onSqlExecuted(String category, long nanos, boolean ok) {
    }
}
