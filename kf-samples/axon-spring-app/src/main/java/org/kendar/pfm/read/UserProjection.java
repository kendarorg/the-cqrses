package org.kendar.pfm.read;

import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.kendar.pfm.domain.events.UserRegistered;
import org.springframework.stereotype.Component;

/**
 * Projects {@code UserRegistered} into the durable {@code pfm_user} read table. The {@code "users"}
 * {@link ProcessingGroup} becomes its own pooled streaming event processor — the unit Axon
 * distributes across nodes via token-store segment claiming (the analog of kf's per-segment
 * ownership). Stateless (all state in the DB) → safe under concurrent segment dispatch and idempotent
 * under at-least-once token replay.
 */
@Component
@ProcessingGroup("users")
public class UserProjection {

    private final UserReadStore store;

    public UserProjection(UserReadStore store) {
        this.store = store;
    }

    @EventHandler
    public void on(UserRegistered e) {
        store.upsert(e.userId, e.username);
    }
}
