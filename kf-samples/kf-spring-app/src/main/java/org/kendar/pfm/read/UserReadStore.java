package org.kendar.pfm.read;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.UUID;

/**
 * Durable read store for the {@code pfm_user} table. A plain Spring bean (not kf-annotated): it is
 * injected into {@link UserProjection}'s handler as a non-first parameter and resolved from
 * {@code GlobalRegistry} via the kf-spring fallback-resolver bridge (and pre-warmed at bootstrap).
 */
@Repository
public class UserReadStore {

    private final JdbcTemplate jdbc;

    public UserReadStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Idempotent upsert (re-applied {@code UserRegistered} events must not fail). */
    public void upsert(UUID userId, String username) {
        jdbc.update(
                "INSERT INTO pfm_user(user_id, username) VALUES (?, ?) "
                        + "ON DUPLICATE KEY UPDATE username = VALUES(username)",
                Uuids.toBytes(userId), username);
    }

    public boolean exists(UUID userId) {
        Integer n = jdbc.queryForObject(
                "SELECT COUNT(*) FROM pfm_user WHERE user_id = ?", Integer.class, Uuids.toBytes(userId));
        return n != null && n > 0;
    }
}
