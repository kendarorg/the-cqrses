package org.kendar.cqrses.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.kendar.cqrses.exceptions.SerializerException;

import static org.junit.jupiter.api.Assertions.*;

class JacksonMessageSerializerTest {

    @Test
    void serializeDeserializeRoundTrip() {
        JacksonMessageSerializer s = new JacksonMessageSerializer();
        byte[] bytes = s.serialize(new Sample("foo", 42));
        Sample back = s.deserialize(bytes, Sample.class);
        assertEquals("foo", back.name);
        assertEquals(42, back.value);
    }

    @Test
    void usesInjectedMapper() {
        ObjectMapper mapper = new ObjectMapper();
        JacksonMessageSerializer s = new JacksonMessageSerializer(mapper);
        assertSame(mapper, s.getMapper());
    }

    @Test
    void deserializeInvalidPayloadThrowsSerializerException() {
        JacksonMessageSerializer s = new JacksonMessageSerializer();
        byte[] junk = new byte[]{(byte) 0xFF, 0x00, 0x12};
        assertThrows(SerializerException.class, () -> s.deserialize(junk, Sample.class));
    }

    @Test
    void serializeUnserializableObjectThrowsSerializerException() {
        JacksonMessageSerializer s = new JacksonMessageSerializer();
        Object unserializable = new Object() {
            @SuppressWarnings("unused")
            public Object getSelf() {
                return this;
            }
        };
        assertThrows(SerializerException.class, () -> s.serialize(unserializable));
    }

    @Test
    void toNodeAndFromNodeRoundTrip() throws Exception {
        JacksonMessageSerializer s = new JacksonMessageSerializer();
        byte[] bytes = s.serialize(new Sample("x", 1));
        var node = s.deserializeToIntermediate(bytes);
        assertEquals("x", node.get("name").asText());
        assertArrayEquals(bytes, s.serialize(node));
    }

    public static class Sample {
        public String name;
        public int value;

        public Sample() {
        }

        public Sample(String name, int value) {
            this.name = name;
            this.value = value;
        }
    }
}
