package org.kendar.cqrses.db;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Pure-Java coverage for the {@code UUID <-> BINARY(16)} conversion: round-trip,
 * null pass-through, fixed 16-byte width, and the big-endian
 * most-significant-then-least-significant byte order the row mappers rely on.
 */
class UuidBytesTest {

    @Test
    void roundTripsAnArbitraryUuid() {
        UUID uuid = UUID.fromString("0190b1c2-d3e4-7f56-89ab-cdef01234567");
        byte[] bytes = UuidBytes.toBytes(uuid);
        assertEquals(16, bytes.length);
        assertEquals(uuid, UuidBytes.fromBytes(bytes));
    }

    @Test
    void roundTripsRandomUuids() {
        for (int i = 0; i < 100; i++) {
            UUID uuid = UUID.randomUUID();
            assertEquals(uuid, UuidBytes.fromBytes(UuidBytes.toBytes(uuid)));
        }
    }

    @Test
    void nullPassesThroughBothWays() {
        assertNull(UuidBytes.toBytes(null));
        assertNull(UuidBytes.fromBytes(null));
    }

    @Test
    void encodesMostSignificantBitsFirstBigEndian() {
        UUID uuid = UUID.randomUUID();
        byte[] bytes = UuidBytes.toBytes(uuid);
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        assertEquals(uuid.getMostSignificantBits(), bb.getLong());
        assertEquals(uuid.getLeastSignificantBits(), bb.getLong());
    }

    @Test
    void zeroUuidEncodesToAllZeroBytes() {
        byte[] bytes = UuidBytes.toBytes(new UUID(0L, 0L));
        assertArrayEquals(new byte[16], bytes);
        assertEquals(new UUID(0L, 0L), UuidBytes.fromBytes(bytes));
    }
}
