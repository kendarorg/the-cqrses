package org.kendar.cqrses.bus;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class InternalMessageTest {

    @Test
    void defaultsAreFalseAndNull() {
        InternalMessage m = new InternalMessage();
        assertFalse(m.isEvent());
        assertNull(m.getPayload());
        assertNull(m.getContext());
    }

    @Test
    void settersRoundTrip() {
        InternalMessage m = new InternalMessage();
        Context ctx = new Context();
        byte[] payload = new byte[]{1, 2, 3};
        m.setEvent(true);
        m.setPayload(payload);
        m.setContext(ctx);

        assertTrue(m.isEvent());
        assertArrayEquals(payload, m.getPayload());
        assertSame(ctx, m.getContext());
    }
}
