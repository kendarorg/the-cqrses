package org.kendar.cqrses.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.annotations.UpcasterSpec;
import org.kendar.cqrses.bus.Context;
import org.kendar.cqrses.bus.InternalMessage;
import org.kendar.cqrses.di.GlobalRegistry;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class UpcastersManagerTest {

    private JacksonMessageSerializer serializer;

    @BeforeEach
    void setUp() {
        GlobalRegistry.clear();
        serializer = new JacksonMessageSerializer();
        GlobalRegistry.register(MessageSerializer.class, serializer);
    }

    @AfterEach
    void tearDown() {
        GlobalRegistry.clear();
    }

    private InternalMessage message(String type, long version, String json) {
        InternalMessage m = new InternalMessage();
        m.setEvent(true);
        m.setPayload(json.getBytes());
        Context ctx = new Context();
        ctx.setType(type);
        ctx.setVersion(version);
        m.setContext(ctx);
        return m;
    }

    private JsonNode node(InternalMessage m) throws Exception {
        return serializer.deserializeToIntermediate(m.getPayload());
    }

    // ── upcasters under test ──────────────────────────────────────────────────

    /** Renames field {@code amount} -> {@code qty} on Incremented v1 -> v2 (version only). */
    public static class RenameField implements Upcaster {
        @UpcasterSpec(origin = "Incremented", from = 1)
        public Object v1tov2(Object nx) {
            var n = (ObjectNode) nx;
            n.set("qty", n.get("amount"));
            n.remove("amount");
            return n;
        }
    }

    /** Two contiguous hops in one class: v2 adds a field, v3 adds another. */
    public static class TwoHops implements Upcaster {
        @UpcasterSpec(origin = "Incremented", from = 2)
        public Object v2tov3(Object nx) {
            var n = (ObjectNode) nx;
            n.put("currency", "EUR");
            return n;
        }

        @UpcasterSpec(origin = "Incremented", from = 3)
        public Object v3tov4(Object nx) {
            var n = (ObjectNode) nx;
            n.put("reason", "manual");
            return n;
        }
    }

    /** A narrow step and a wider direct jump from the same version. */
    public static class StepAndJump implements Upcaster {
        @UpcasterSpec(origin = "Jumpy", from = 1)
        public Object step(Object nx) {
            var n = (ObjectNode) nx;
            n.put("path", "step");
            return n;
        }

        @UpcasterSpec(origin = "Jumpy", from = 1, to = 5)
        public Object jump(Object nx) {
            var n = (ObjectNode) nx;
            n.put("path", "jump");
            return n;
        }
    }

    /** Renames the type itself: OldName v1 -> NewName v2. */
    public static class RenameType implements Upcaster {
        @UpcasterSpec(origin = "OldName", from = 1, target = "NewName")
        public Object rename(Object n) {
            return n;
        }
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    @Test
    void emptyManagerPassesThrough() {
        UpcastersManager mgr = new UpcastersManager(new JacksonMessageSerializer(),List.of());
        InternalMessage in = message("Incremented", 1, "{\"amount\":5}");
        InternalMessage out = mgr.upcast(in);
        assertSame(in, out);
        assertEquals(1L, out.getContext().getVersion());
    }

    @Test
    void unmatchedTypePassesThrough() throws Exception {
        UpcastersManager mgr = new UpcastersManager(new JacksonMessageSerializer(),List.of(new RenameField()));
        InternalMessage out = mgr.upcast(message("Unrelated", 1, "{\"amount\":5}"));
        assertEquals(1L, out.getContext().getVersion());
        assertEquals(5, node(out).get("amount").asInt());
    }

    @Test
    void singleHopRewritesFieldAndBumpsVersion() throws Exception {
        UpcastersManager mgr = new UpcastersManager(new JacksonMessageSerializer(),List.of(new RenameField()));
        InternalMessage out = mgr.upcast(message("Incremented", 1, "{\"amount\":5}"));

        assertEquals(2L, out.getContext().getVersion());
        assertEquals("Incremented", out.getContext().getType(), "type unchanged when target is \"\"");
        assertFalse(node(out).has("amount"));
        assertEquals(5, node(out).get("qty").asInt());
    }

    @Test
    void matchingIsCaseInsensitive() throws Exception {
        UpcastersManager mgr = new UpcastersManager(new JacksonMessageSerializer(),List.of(new RenameField()));
        InternalMessage out = mgr.upcast(message("incremented", 1, "{\"amount\":5}"));
        assertEquals(2L, out.getContext().getVersion());
        assertEquals(5, node(out).get("qty").asInt());
    }

    @Test
    void chainsAcrossMultipleHopsToTheTop() throws Exception {
        UpcastersManager mgr = new UpcastersManager(new JacksonMessageSerializer(),List.of(new RenameField(), new TwoHops()));
        InternalMessage out = mgr.upcast(message("Incremented", 1, "{\"amount\":5}"));

        assertEquals(4L, out.getContext().getVersion(), "v1 -> v2 -> v3 -> v4");
        JsonNode n = node(out);
        assertEquals(5, n.get("qty").asInt());
        assertEquals("EUR", n.get("currency").asText());
        assertEquals("manual", n.get("reason").asText());
    }

    @Test
    void widestJumpWinsWhenSeveralMatch() throws Exception {
        UpcastersManager mgr = new UpcastersManager(new JacksonMessageSerializer(),List.of(new StepAndJump()));
        InternalMessage out = mgr.upcast(message("Jumpy", 1, "{}"));

        assertEquals(5L, out.getContext().getVersion(), "direct v1->v5 jump beats the v1->v2 step");
        assertEquals("jump", node(out).get("path").asText());
    }

    @Test
    void targetRenamesTheType() throws Exception {
        UpcastersManager mgr = new UpcastersManager(new JacksonMessageSerializer(),List.of(new RenameType()));
        InternalMessage out = mgr.upcast(message("OldName", 1, "{}"));

        assertEquals("NewName", out.getContext().getType());
        assertEquals(2L, out.getContext().getVersion());
    }

    @Test
    void duplicateHopIsRejectedAtSetup() {
        class Dup implements Upcaster {
            @UpcasterSpec(origin = "X", from = 1, to = 2)
            public Object a(Object n) { return n; }

            @UpcasterSpec(origin = "x", from = 1, to = 2)
            public Object b(Object n) { return n; }
        }
        assertThrows(IllegalStateException.class, () -> new UpcastersManager(new JacksonMessageSerializer(),List.of(new Dup())));
    }

    @Test
    void nonAdvancingHopIsRejectedAtSetup() {
        class Bad implements Upcaster {
            @UpcasterSpec(origin = "X", from = 3, to = 3)
            public Object a(Object n) { return n; }
        }
        assertThrows(IllegalStateException.class, () -> new UpcastersManager(new JacksonMessageSerializer(),List.of(new Bad())));
    }

    @Test
    void badSignatureIsRejectedAtSetup() {
        class Bad implements Upcaster {
            @UpcasterSpec(origin = "X", from = 1)
            public String a(Object n,String test) { return ""; }
        }
        assertThrows(IllegalStateException.class, () -> new UpcastersManager(new JacksonMessageSerializer(),List.of(new Bad())));
    }
}
