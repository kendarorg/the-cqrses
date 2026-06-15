package org.kendar.pfm.read;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.UUID;

/**
 * Durable read store for the {@code pfm_user} table. Identical to the kf sample's store: a plain
 * Spring bean over {@link JdbcTemplate}, idempotent upsert (a re-applied {@code UserRegistered}
 * event must not fail). Injected directly into {@code UserProjection}.
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
