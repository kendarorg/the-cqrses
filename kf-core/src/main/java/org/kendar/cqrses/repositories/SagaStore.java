package org.kendar.cqrses.repositories;

import java.util.Optional;

public interface SagaStore {
    void storeSaga(Object saga);

    Optional<SagaInstance> loadSaga(String sagaId);

    Optional<SagaInstance> loadSagaByCorrelationId(String correlationId, String type);

    /**
     * Remove the saga instance and any correlation entries pointing at it.
     * Called by the dispatcher after a {@code @SagaHandler} returned with
     * {@link org.kendar.cqrses.saga.SagaManager#isSagaEnded()} on a saga whose
     * {@code @Saga(deleteAfterCompletion = true)}. Implementations must be
     * idempotent — a saga that no longer exists is not an error.
     */
    default void deleteSaga(Object saga) {
    }


    /**
     * Hook called by the cluster pause path (W4) before {@code PausedAck} is
     * published so any buffered saga writes for {@code processingGroup} land
     * durably and are visible to the new owner after a rebalance. Synchronous
     * by design — pause latency reflects flush latency. Default no-op for
     * stores that already write through ({@code JdbcSagaStore},
     * {@code InMemorySagaStore}).
     */
    default void flush(String processingGroup) {
    }
}
