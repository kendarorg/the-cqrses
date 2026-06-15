package org.kendar.cqrses.repositories;

import org.kendar.cqrses.di.GlobalRegistry;
import org.kendar.cqrses.pg.SegmentCalculator;
import org.kendar.cqrses.serialization.MessageSerializer;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Process-local {@link SagaStore}, partitioned by {@code segment(sagaId)}. Each
 * segment partition co-locates a saga's instance, its reverse correlation index
 * (owned values) and its correlation entries, so a future cluster can migrate an
 * instance plus its correlations as one {@code (group, segment(sagaId))} unit. A
 * node-local global {@code (type, value) → sagaId} index resolves lookups first.
 * Reflection caches for {@code @SagaId} and the correlation specs live in
 * {@link BaseSagaStore}.
 */
public class InMemorySagaStore extends BaseSagaStore {

    private final ConcurrentHashMap<Integer, ConcurrentHashMap<InstanceKey, SagaInstance>> sagaInstancesBySegment = new ConcurrentHashMap<>();
    // Reverse index so a re-store can drop correlation entries that this saga
    // no longer carries — otherwise stale (type, value) keys keep pointing at
    // the saga and shadow whichever saga later claims that value. Co-located.
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<InstanceKey, Set<String>>> ownedBySegment = new ConcurrentHashMap<>();
    // Co-located correlation entries (for cluster-migration semantics).
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<CorrelationKey, String>> correlationsBySegment = new ConcurrentHashMap<>();
    // Node-local global (type, value) -> sagaId; resolves lookups FIRST.
    private final ConcurrentHashMap<CorrelationKey, String> correlationIndex = new ConcurrentHashMap<>();

    private static int segment(String id) {
        return SegmentCalculator.calculateSegment(id);
    }

    @Override
    public void storeSaga(Object saga) {
        Objects.requireNonNull(saga, "saga");
        String id = extractSagaId(saga);
        if (id == null) {
            throw new IllegalStateException(
                    "Saga " + saga.getClass().getName() + " has a null @SagaId value");
        }
        String type = extractSagaType(saga);
        InstanceKey instanceKey = new InstanceKey(type, id);
        int seg = segment(id);

        byte[] content = GlobalRegistry.get(MessageSerializer.class).serialize(saga);

        var newCorrelations = extractCorrelations(saga);

        SagaInstance instance = new SagaInstance();
        instance.setId(id);
        instance.setType(type);
        instance.setContent(content);
        // SagaInstance stores a single canonical correlationId. Use the first
        // correlation value if any; lookups by correlation set the field on
        // the returned instance to the value that actually matched.
        instance.setCorrelationId(newCorrelations.isEmpty() ? null : newCorrelations.get(0).value());
        sagaInstancesBySegment.computeIfAbsent(seg, k -> new ConcurrentHashMap<>()).put(instanceKey, instance);

        Set<String> newValues = new HashSet<>();
        for (var corr : newCorrelations) newValues.add(corr.value());

        var partitionCorrelations = correlationsBySegment.computeIfAbsent(seg, k -> new ConcurrentHashMap<>());
        ownedBySegment.computeIfAbsent(seg, k -> new ConcurrentHashMap<>()).compute(instanceKey, (k, oldValues) -> {
            if (oldValues != null) {
                for (String v : oldValues) {
                    if (!newValues.contains(v)) {
                        CorrelationKey ck = new CorrelationKey(type, v);
                        partitionCorrelations.remove(ck, id);
                        correlationIndex.remove(ck, id);
                    }
                }
            }
            for (String v : newValues) {
                CorrelationKey ck = new CorrelationKey(type, v);
                partitionCorrelations.put(ck, id);
                correlationIndex.put(ck, id);
            }
            return newValues;
        });
    }

    @Override
    public void deleteSaga(Object saga) {
        Objects.requireNonNull(saga, "saga");
        String id = extractSagaId(saga);
        if (id == null) return;
        String type = extractSagaType(saga);
        InstanceKey instanceKey = new InstanceKey(type, id);
        int seg = segment(id);
        var instances = sagaInstancesBySegment.get(seg);
        if (instances != null) instances.remove(instanceKey);
        var owned = ownedBySegment.get(seg);
        Set<String> ownedValues = owned == null ? null : owned.remove(instanceKey);
        if (ownedValues != null) {
            var partitionCorrelations = correlationsBySegment.get(seg);
            for (String v : ownedValues) {
                CorrelationKey ck = new CorrelationKey(type, v);
                correlationIndex.remove(ck, id);
                if (partitionCorrelations != null) partitionCorrelations.remove(ck, id);
            }
        }
    }

    /**
     * Load by {@code @SagaId} — real, stable identity. The returned instance's
     * {@code correlationId} is the canonical value (first correlation at store
     * time) and <strong>may differ</strong> from the one returned by
     * {@link #loadSagaByCorrelationId(String, String)}, which echoes back the
     * value that actually matched. {@code SagaInstance.correlationId} is a lookup
     * echo, not identity, so no contract test asserts the two are equal. Mirrors
     * JDBC, where {@code saga_instance.correlation_id} is one column and lookups
     * resolve through {@code saga_correlation}.
     */
    @Override
    public Optional<SagaInstance> loadSaga(String sagaId) {
        if (sagaId == null) return Optional.empty();
        // Caller has no type — scan this segment's instance entries by id. Saga IDs
        // are expected to be unique across types in practice.
        var instances = sagaInstancesBySegment.get(segment(sagaId));
        if (instances == null) return Optional.empty();
        for (var entry : instances.entrySet()) {
            if (entry.getKey().id().equals(sagaId)) return Optional.of(entry.getValue());
        }
        return Optional.empty();
    }

    /**
     * Resolve by a correlation {@code (type, value)}. The returned view carries
     * {@code correlationId == correlationId} (the value the caller looked up),
     * which for a multi-correlation saga <strong>may differ</strong> from the
     * canonical value seen via {@link #loadSaga(String)}. The {@code EventBus}
     * only ever resolves sagas through this method and never trusts
     * {@code SagaInstance.correlationId} as identity, so the asymmetry is free.
     */
    @Override
    public Optional<SagaInstance> loadSagaByCorrelationId(String correlationId, String type) {
        if (correlationId == null || type == null) return Optional.empty();
        String id = correlationIndex.get(new CorrelationKey(type, correlationId));
        if (id == null) return Optional.empty();
        var instances = sagaInstancesBySegment.get(segment(id));
        SagaInstance stored = instances == null ? null : instances.get(new InstanceKey(type, id));
        if (stored == null) return Optional.empty();
        // Reflect the value the caller looked up — callers (e.g. EventBus) only
        // see the single correlationId on SagaInstance and shouldn't have to
        // know which of the saga's correlation values won the lookup.
        SagaInstance view = new SagaInstance();
        view.setId(stored.getId());
        view.setType(stored.getType());
        view.setContent(stored.getContent());
        view.setCorrelationId(correlationId);
        return Optional.of(view);
    }

    private record InstanceKey(String type, String id) {
    }

    private record CorrelationKey(String type, String value) {
    }
}
