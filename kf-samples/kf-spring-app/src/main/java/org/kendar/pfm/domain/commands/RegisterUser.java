package org.kendar.pfm.domain.commands;

import org.kendar.cqrses.annotations.AggregateIdentifier;
import org.kendar.cqrses.annotations.Command;

import java.util.UUID;

/**
 * Login/registration command. Idempotent: the {@code Account} aggregate ignores it once already
 * registered, so logging in repeatedly with the same username is a no-op.
 */
@Command(version = 1)
public class RegisterUser {
    @AggregateIdentifier
    public UUID userId;
    public String username;

    public RegisterUser() {
    }

    public RegisterUser(UUID userId, String username) {
        this.userId = userId;
        this.username = username;
    }
}
