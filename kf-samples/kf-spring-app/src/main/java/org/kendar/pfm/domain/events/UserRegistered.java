package org.kendar.pfm.domain.events;

import org.kendar.cqrses.annotations.AggregateIdentifier;
import org.kendar.cqrses.annotations.Event;

import java.util.UUID;

@Event(version = 1)
public class UserRegistered {
    @AggregateIdentifier
    public UUID userId;
    public String username;

    public UserRegistered() {
    }

    public UserRegistered(UUID userId, String username) {
        this.userId = userId;
        this.username = username;
    }
}
