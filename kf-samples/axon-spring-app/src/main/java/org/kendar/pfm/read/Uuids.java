package org.kendar.pfm.read;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/** UUID ⇄ BINARY(16) helpers + the deterministic username→userId mapping. */
public final class Uuids {

    private Uuids() {
    }

    /** Stable name-based userId so the HTTP layer can address a user's stream from the username alone. */
    public static UUID userId(String username) {
        return UUID.nameUUIDFromBytes(("pfm:" + username).getBytes(StandardCharsets.UTF_8));
    }

    public static byte[] toBytes(UUID id) {
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(id.getMostSignificantBits());
        bb.putLong(id.getLeastSignificantBits());
        return bb.array();
    }

    public static UUID fromBytes(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        return new UUID(bb.getLong(), bb.getLong());
    }
}
