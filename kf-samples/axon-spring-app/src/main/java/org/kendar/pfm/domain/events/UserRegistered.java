package org.kendar.pfm.domain.events;

import java.util.UUID;

public class UserRegistered {
    public UUID userId;
    public String username;

    public UserRegistered() {
    }

    public UserRegistered(UUID userId, String username) {
        this.userId = userId;
        this.username = username;
    }
}
