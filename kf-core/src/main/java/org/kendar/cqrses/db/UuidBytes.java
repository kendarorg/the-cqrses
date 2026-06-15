package org.kendar.cqrses.db;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * UUID &harr; {@code BINARY(16)} conversion for the JDBC row mappers. We store
 * UUIDs as 16 raw bytes (big-endian most-significant then least-significant
 * long) rather than {@code CHAR(36)} — compact and portable across H2 and
 * MySQL. See {@code docs/tricks.md}.
 */
public final class UuidBytes {
    private UuidBytes() {
    }

    public static byte[] toBytes(UUID uuid) {
        if (uuid == null) return null;
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }

    public static UUID fromBytes(byte[] bytes) {
        if (bytes == null) return null;
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long hi = bb.getLong();
        long lo = bb.getLong();
        return new UUID(hi, lo);
    }
}
