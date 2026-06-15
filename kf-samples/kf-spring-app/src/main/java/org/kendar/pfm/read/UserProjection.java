package org.kendar.pfm.read;

import org.kendar.cqrses.annotations.EventHandler;
import org.kendar.cqrses.annotations.Projection;
import org.kendar.pfm.domain.events.UserRegistered;
import org.springframework.stereotype.Component;

/**
 * Projects {@code UserRegistered} into the durable {@code pfm_user} read table. A live Spring bean:
 * the scanner finds {@code @Projection} and a bean of this type exists, so it is bridged via
 * {@code GlobalRegistry.register(class, bean)}. Stateless (all state in the DB) → safe under
 * concurrent lane dispatch.
 */
@Component
@Projection(group = "users")
public class UserProjection {

    @EventHandler
    public void on(UserRegistered e, UserReadStore store) {
        store.upsert(e.userId, e.username);
    }
}
