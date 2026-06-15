package org.kendar.cqrses.cluster.forwarding;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Binary framing for the command-forwarding channel. All integers big-endian
 * ({@link DataInputStream} / {@link DataOutputStream} semantics); one TCP
 * connection is fully multiplexed by the {@code requestId} correlation id.
 *
 * <p><b>Request frame</b> (client → owner):
 * <pre>
 *   kind      u8   'C' = command ('E' reserved for future event forwarding)
 *   mode      u8   'W' = wait for the handler result, 'A' = ack on receipt
 *   requestId i64  per-connection monotonic counter
 *   bodyLen   i32  byte length of the records that follow
 *   records:  each is  tag u8 | len i32 | len bytes
 *     'T'  command SIMPLE class name, UTF-8 (the framework's canonical type identity)
 *     'V'  expected aggregateVersion, i64 payload (len = 8; -1 sentinel = assign next)
 *     'P'  command payload, serialized by the registered MessageSerializer
 * </pre>
 *
 * <p><b>Response frame</b> (owner → client):
 * <pre>
 *   kind      u8   'R'
 *   requestId i64
 *   status    u8   'A' = ack (mode A accepted / mode W void result)
 *                  'V' = value follows, 'E' = remote handler threw
 *   bodyLen   i32
 *   records ('V'): 'T' result class FQCN, 'P' serialized result
 *   records ('E'): 'C' exception class FQCN, 'M' exception message
 * </pre>
 *
 * <p>Every record — request and response alike — is uniformly
 * {@code tag | len | bytes}, so a decoder skips tags it does not know (e.g. the
 * reserved {@code 'I'} traceId record) without understanding them: forward
 * compatibility within the same frame version. A truncated stream surfaces as
 * {@link EOFException} from the read methods; the caller treats it as a dead
 * connection. {@code bodyLen} is capped to keep a corrupt length prefix from
 * provoking a giant allocation.
 */
public final class WireCodec {

    public static final byte KIND_COMMAND = 'C';
    public static final byte KIND_EVENT_RESERVED = 'E';
    public static final byte KIND_RESPONSE = 'R';

    public static final byte MODE_WAIT = 'W';
    public static final byte MODE_ACK = 'A';

    public static final byte STATUS_ACK = 'A';
    public static final byte STATUS_VALUE = 'V';
    public static final byte STATUS_ERROR = 'E';

    private static final byte TAG_TYPE = 'T';
    private static final byte TAG_VERSION = 'V';
    private static final byte TAG_PAYLOAD = 'P';
    private static final byte TAG_ERROR_CLASS = 'C';
    private static final byte TAG_ERROR_MESSAGE = 'M';

    /** Upper bound on any single frame body / record (64 MiB) — corrupt-length guard. */
    static final int MAX_LEN = 64 * 1024 * 1024;

    private WireCodec() {
    }

    /**
     * A command crossing the wire. {@code mode} is {@link #MODE_WAIT} or
     * {@link #MODE_ACK}; {@code type} the command's simple class name;
     * {@code payload} the serialized command.
     */
    public record CommandRequest(byte mode, long requestId, String type, long aggregateVersion, byte[] payload) {
    }

    /**
     * The owner's answer. Exactly one shape per {@code status}:
     * {@link #STATUS_ACK} carries nothing, {@link #STATUS_VALUE} carries
     * {@code resultType} (FQCN) + {@code resultPayload}, {@link #STATUS_ERROR}
     * carries {@code errorClass} (FQCN) + {@code errorMessage}.
     */
    public record CommandResponse(long requestId, byte status,
                                  String resultType, byte[] resultPayload,
                                  String errorClass, String errorMessage) {

        public static CommandResponse ack(long requestId) {
            return new CommandResponse(requestId, STATUS_ACK, null, null, null, null);
        }

        public static CommandResponse value(long requestId, String resultType, byte[] resultPayload) {
            return new CommandResponse(requestId, STATUS_VALUE, resultType, resultPayload, null, null);
        }

        public static CommandResponse error(long requestId, String errorClass, String errorMessage) {
            return new CommandResponse(requestId, STATUS_ERROR, null, null, errorClass, errorMessage);
        }
    }

    public static void writeRequest(DataOutputStream out, CommandRequest request) throws IOException {
        var body = new ByteArrayOutputStream();
        var bodyOut = new DataOutputStream(body);
        writeRecord(bodyOut, TAG_TYPE, request.type().getBytes(StandardCharsets.UTF_8));
        writeLongRecord(bodyOut, TAG_VERSION, request.aggregateVersion());
        writeRecord(bodyOut, TAG_PAYLOAD, request.payload());

        out.writeByte(KIND_COMMAND);
        out.writeByte(request.mode());
        out.writeLong(request.requestId());
        out.writeInt(body.size());
        body.writeTo(out);
        out.flush();
    }

    /** @throws EOFException on a cleanly-closed or truncated stream — dead connection. */
    public static CommandRequest readRequest(DataInputStream in) throws IOException {
        byte kind = in.readByte();
        if (kind != KIND_COMMAND) {
            throw new IOException("unsupported request kind: " + (char) kind);
        }
        byte mode = in.readByte();
        if (mode != MODE_WAIT && mode != MODE_ACK) {
            throw new IOException("unsupported request mode: " + (char) mode);
        }
        long requestId = in.readLong();
        var records = readRecords(in);

        byte[] type = records.get(TAG_TYPE);
        byte[] version = records.get(TAG_VERSION);
        byte[] payload = records.get(TAG_PAYLOAD);
        if (type == null || payload == null) {
            throw new IOException("request " + requestId + " missing T or P record");
        }
        long aggregateVersion = version == null ? -1L : toLong(version);
        return new CommandRequest(mode, requestId, new String(type, StandardCharsets.UTF_8),
                aggregateVersion, payload);
    }

    public static void writeResponse(DataOutputStream out, CommandResponse response) throws IOException {
        var body = new ByteArrayOutputStream();
        var bodyOut = new DataOutputStream(body);
        if (response.status() == STATUS_VALUE) {
            writeRecord(bodyOut, TAG_TYPE, response.resultType().getBytes(StandardCharsets.UTF_8));
            writeRecord(bodyOut, TAG_PAYLOAD, response.resultPayload());
        } else if (response.status() == STATUS_ERROR) {
            writeRecord(bodyOut, TAG_ERROR_CLASS, response.errorClass().getBytes(StandardCharsets.UTF_8));
            writeRecord(bodyOut, TAG_ERROR_MESSAGE,
                    (response.errorMessage() == null ? "" : response.errorMessage()).getBytes(StandardCharsets.UTF_8));
        }

        out.writeByte(KIND_RESPONSE);
        out.writeLong(response.requestId());
        out.writeByte(response.status());
        out.writeInt(body.size());
        body.writeTo(out);
        out.flush();
    }

    /** @throws EOFException on a cleanly-closed or truncated stream — dead connection. */
    public static CommandResponse readResponse(DataInputStream in) throws IOException {
        byte kind = in.readByte();
        if (kind != KIND_RESPONSE) {
            throw new IOException("unsupported response kind: " + (char) kind);
        }
        long requestId = in.readLong();
        byte status = in.readByte();
        var records = readRecords(in);

        return switch (status) {
            case STATUS_ACK -> CommandResponse.ack(requestId);
            case STATUS_VALUE -> {
                byte[] type = records.get(TAG_TYPE);
                byte[] payload = records.get(TAG_PAYLOAD);
                if (type == null || payload == null) {
                    throw new IOException("value response " + requestId + " missing T or P record");
                }
                yield CommandResponse.value(requestId, new String(type, StandardCharsets.UTF_8), payload);
            }
            case STATUS_ERROR -> {
                byte[] cls = records.get(TAG_ERROR_CLASS);
                byte[] msg = records.get(TAG_ERROR_MESSAGE);
                if (cls == null) {
                    throw new IOException("error response " + requestId + " missing C record");
                }
                yield CommandResponse.error(requestId, new String(cls, StandardCharsets.UTF_8),
                        msg == null ? "" : new String(msg, StandardCharsets.UTF_8));
            }
            default -> throw new IOException("unsupported response status: " + (char) status);
        };
    }

    private static void writeRecord(DataOutputStream out, byte tag, byte[] bytes) throws IOException {
        out.writeByte(tag);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static void writeLongRecord(DataOutputStream out, byte tag, long value) throws IOException {
        out.writeByte(tag);
        out.writeInt(Long.BYTES);
        out.writeLong(value);
    }

    /** Read the {@code bodyLen}-prefixed record list, last-tag-wins, unknown tags kept (and ignored upstream). */
    private static java.util.Map<Byte, byte[]> readRecords(DataInputStream in) throws IOException {
        int bodyLen = in.readInt();
        if (bodyLen < 0 || bodyLen > MAX_LEN) {
            throw new IOException("frame body length out of bounds: " + bodyLen);
        }
        byte[] bodyBytes = new byte[bodyLen];
        in.readFully(bodyBytes);

        var records = new java.util.HashMap<Byte, byte[]>();
        var body = new DataInputStream(new ByteArrayInputStream(bodyBytes));
        while (body.available() > 0) {
            byte tag = body.readByte();
            int len = body.readInt();
            if (len < 0 || len > body.available()) {
                throw new IOException("record '" + (char) tag + "' length out of bounds: " + len);
            }
            byte[] bytes = new byte[len];
            body.readFully(bytes);
            records.put(tag, bytes);
        }
        return records;
    }

    private static long toLong(byte[] bytes) throws IOException {
        if (bytes.length != Long.BYTES) {
            throw new IOException("V record must be 8 bytes, got " + bytes.length);
        }
        return new DataInputStream(new ByteArrayInputStream(bytes)).readLong();
    }
}
