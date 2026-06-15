package org.kendar.cqrses.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Aggregate {
    String group() default "default";

    /**
     * Snapshot schema revision of this aggregate. Stored on every snapshot row
     * ({@code snapshot_entry.schema_version}); bump it whenever the snapshot
     * payload's shape changes. On rehydrate a stored snapshot whose revision is
     * older than this value is run through the registered upcaster chain
     * ({@code @UpcasterSpec(origin = "<snapshot payload simple name>", from, to)});
     * if no chain brings it up to the current revision the snapshot is discarded
     * and the aggregate replays its full event stream (snapshots are
     * best-effort), so a bump without an upcaster is safe — just slower until
     * the next snapshot is written.
     */
    int version() default 1;

    /**
     * Automatic snapshot threshold: when {@code > 0}, the command pipeline
     * stores a snapshot every time the aggregate's stream crosses a multiple of
     * this many events. Requires the aggregate to expose the snapshot pair
     * {@code T getSnapshot()} / {@code setSnapshot(T)} (validated at
     * registration). {@code 0} (the default) disables automatic snapshotting;
     * manual {@code EventStore.storeSnapshot} calls remain available either way.
     */
    int snapshotEvery() default 0;
}
