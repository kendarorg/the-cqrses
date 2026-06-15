package org.kendar.pfm.domain.commands;

import org.axonframework.modelling.command.TargetAggregateIdentifier;

import java.util.UUID;

/**
 * Login/registration command. Idempotent: the {@code Account} aggregate handles it with
 * {@code CREATE_IF_MISSING} and ignores it once already registered, so logging in repeatedly with
 * the same username is a no-op.
 */
public class RegisterUser {
    @TargetAggregateIdentifier
    public UUID userId;
    public String username;

    public RegisterUser() {
    }

    public RegisterUser(UUID userId, String username) {
        this.userId = userId;
        this.username = username;
    }
}
