package org.kendar.pfm.web.dto;

/** {@code created} is true when this login registered a brand-new user. */
public record LoginResponse(String userId, String username, boolean created) {
}
