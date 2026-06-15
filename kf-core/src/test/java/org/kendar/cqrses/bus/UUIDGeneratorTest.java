package org.kendar.cqrses.bus;

import org.junit.jupiter.api.Test;
import org.kendar.cqrses.utils.UUIDGenerator;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class UUIDGeneratorTest {

    @Test
    void returnsNonNullUuid() {
        assertNotNull(UUIDGenerator.newUuid());
    }

    @Test
    void successiveCallsReturnDifferentUuids() {
        UUID a = UUIDGenerator.newUuid();
        UUID b = UUIDGenerator.newUuid();
        assertNotEquals(a, b);
    }
}
