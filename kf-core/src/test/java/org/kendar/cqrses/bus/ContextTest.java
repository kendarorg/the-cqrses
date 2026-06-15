package org.kendar.cqrses.bus;

import org.junit.jupiter.api.Test;
import org.kendar.cqrses.utils.UUIDGenerator;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ContextTest {

    @Test
    void defaultsAggregateVersionToMinusOne() {
        assertEquals(-1L, new Context().getAggregateVersion());
    }

    @Test
    void defaultsTimestampToNow() {
        Instant before = Instant.now();
        Context ctx = new Context();
        Instant after = Instant.now();
        assertNotNull(ctx.getTimestamp());
        assertFalse(ctx.getTimestamp().isBefore(before));
        assertFalse(ctx.getTimestamp().isAfter(after));
    }

    @Test
    void emptyMetadataMapByDefault() {
        assertNotNull(new Context().getMetadata());
        assertTrue(new Context().getMetadata().isEmpty());
    }

    @Test
    void settersAndGettersRoundTrip() {
        Context ctx = new Context();
        UUID aggId = UUIDGenerator.newUuid();
        UUID traceId = UUIDGenerator.newUuid();
        Instant ts = Instant.parse("2026-01-01T00:00:00Z");
        Map<String, String> md = Map.of("k", "v");
        ctx.setAggregateId(aggId);
        ctx.setAggregateVersion(7L);
        ctx.setType("MyType");
        ctx.setVersion(3L);
        ctx.setTraceId(traceId);
        ctx.setMetadata(md);
        ctx.setTimestamp(ts);

        assertEquals(aggId, ctx.getAggregateId());
        assertEquals(7L, ctx.getAggregateVersion());
        assertEquals("MyType", ctx.getType());
        assertEquals(3L, ctx.getVersion());
        assertEquals(traceId, ctx.getTraceId());
        assertEquals(md, ctx.getMetadata());
        assertEquals(ts, ctx.getTimestamp());
    }
}
