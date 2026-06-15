package org.kendar.cqrses.observability;

/**
 * Test adapter implementing only the original abstract methods as no-ops.
 * Compiling at all is itself the contract that the newer hooks
 * ({@code onAppendPhase}, {@code onAppendInFlight}, {@code onPumpNudge},
 * {@code onPumpLag}) stay {@code default} — pre-existing implementations must
 * keep building unchanged. Subclass and override what the test records.
 */
public class TestObservabilityAdapter implements ObservabilityInterface {

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
