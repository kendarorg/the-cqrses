package org.kendar.cqrses.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.kendar.cqrses.exceptions.SerializerException;

import java.io.IOException;

public class JacksonMessageSerializer implements MessageSerializer<String,JsonNode> {

    private ObjectMapper mapper;

    public JacksonMessageSerializer(ObjectMapper mapper) {
        // JSON is the only serialization format in the framework, so the JSR-310
        // module is registered unconditionally: any user payload (or the DLQ's
        // Context envelope) carrying a java.time field — Instant, LocalDate, … —
        // would otherwise throw at serialize time. See docs/tricks.md.
        this.mapper = mapper.registerModule(new JavaTimeModule());
    }

    public JacksonMessageSerializer() {
        this(new ObjectMapper());
    }

    @Override
    public byte[] serialize(Object domainObject)  {
        try {
            return mapper.writeValueAsBytes(domainObject);
        } catch (Exception ex) {
            throw new SerializerException(ex);
        }
    }

    @Override
    public <T> T deserialize(byte[] payload, Class<T> targetClass)  {
        try {
            return mapper.readValue(payload, targetClass);
        } catch (Exception ex) {
            throw new SerializerException(ex);
        }
    }

    @Override
    public String serializeToFormat(Object domainObject)  {
        try {
            return mapper.writeValueAsString(domainObject);
        } catch (JsonProcessingException e) {
            throw new SerializerException(e);
        }
    }

    @Override
    public <T> T deserializeFromFormat(String payload, Class<T> targetClass)  {
        try {
            return mapper.readValue(payload, targetClass);
        } catch (JsonProcessingException e) {
            throw new SerializerException(e);
        }
    }

    @Override
    public String deserializeToFormat(byte[] payload)  {
        return new String(payload);
    }

    @Override
    public JsonNode deserializeToIntermediate(byte[] payload) {
        try {
            return mapper.readTree(payload);
        } catch (IOException e) {
            throw new SerializerException(e);
        }
    }
//
//    @Override
//    public JsonNode serializeToFormat(Object domainObject)  {
//        return null;
//    }
//
//    @Override
//    public JsonNode deserializeToFormat(byte[] payload)  {
//        return null;
//    }
//
//    @Override
//    public Object deserializeFromFormat(Object payload, Class targetClass)  {
//        return null;
//    }
//
//
//    /**
//     * Parse payload bytes to a JsonNode for upcaster use.
//     */
//    public JsonNode toNode(byte[] payload) throws Exception {
//        return mapper.readTree(payload);
//    }
//
//    /**
//     * Encode a JsonNode back to payload bytes.
//     */
//    public byte[] fromNode(JsonNode node) throws Exception {
//        return mapper.writeValueAsBytes(node);
//    }

    public ObjectMapper getMapper() {
        return mapper;
    }
}
