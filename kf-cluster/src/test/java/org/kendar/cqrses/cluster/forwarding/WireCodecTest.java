package org.kendar.cqrses.cluster.forwarding;

import org.junit.jupiter.api.Test;
import org.kendar.cqrses.cluster.forwarding.WireCodec.CommandRequest;
import org.kendar.cqrses.cluster.forwarding.WireCodec.CommandResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class WireCodecTest {

    private static byte[] writeRequest(CommandRequest request) throws IOException {
        var out = new ByteArrayOutputStream();
        WireCodec.writeRequest(new DataOutputStream(out), request);
        return out.toByteArray();
    }

    private static byte[] writeResponse(CommandResponse response) throws IOException {
        var out = new ByteArrayOutputStream();
        WireCodec.writeResponse(new DataOutputStream(out), response);
        return out.toByteArray();
    }

    @Test
    void requestRoundTrip() throws IOException {
        var request = new CommandRequest(WireCodec.MODE_WAIT, 42L, "RecordOperation", 7L,
                "{\"amount\":5}".getBytes(StandardCharsets.UTF_8));
        var read = WireCodec.readRequest(new DataInputStream(new ByteArrayInputStream(writeRequest(request))));

        assertEquals(WireCodec.MODE_WAIT, read.mode());
        assertEquals(42L, read.requestId());
        assertEquals("RecordOperation", read.type());
        assertEquals(7L, read.aggregateVersion());
        assertArrayEquals(request.payload(), read.payload());
    }

    @Test
    void requestRoundTripAckModeAndSentinelVersion() throws IOException {
        var request = new CommandRequest(WireCodec.MODE_ACK, 1L, "X", -1L, new byte[0]);
        var read = WireCodec.readRequest(new DataInputStream(new ByteArrayInputStream(writeRequest(request))));
        assertEquals(WireCodec.MODE_ACK, read.mode());
        assertEquals(-1L, read.aggregateVersion());
        assertEquals(0, read.payload().length);
    }

    @Test
    void responseAckRoundTrip() throws IOException {
        var read = WireCodec.readResponse(new DataInputStream(
                new ByteArrayInputStream(writeResponse(CommandResponse.ack(9L)))));
        assertEquals(9L, read.requestId());
        assertEquals(WireCodec.STATUS_ACK, read.status());
    }

    @Test
    void responseValueRoundTrip() throws IOException {
        var response = CommandResponse.value(10L, "java.util.UUID",
                "\"d3b0...\"".getBytes(StandardCharsets.UTF_8));
        var read = WireCodec.readResponse(new DataInputStream(
                new ByteArrayInputStream(writeResponse(response))));
        assertEquals(WireCodec.STATUS_VALUE, read.status());
        assertEquals("java.util.UUID", read.resultType());
        assertArrayEquals(response.resultPayload(), read.resultPayload());
    }

    @Test
    void responseErrorRoundTripIncludingNullMessage() throws IOException {
        var read = WireCodec.readResponse(new DataInputStream(new ByteArrayInputStream(
                writeResponse(CommandResponse.error(11L, "x.y.Boom", "it broke")))));
        assertEquals(WireCodec.STATUS_ERROR, read.status());
        assertEquals("x.y.Boom", read.errorClass());
        assertEquals("it broke", read.errorMessage());

        var nullMsg = WireCodec.readResponse(new DataInputStream(new ByteArrayInputStream(
                writeResponse(CommandResponse.error(12L, "x.y.Boom", null)))));
        assertEquals("", nullMsg.errorMessage());
    }

    @Test
    void unknownRecordTagIsSkipped() throws IOException {
        // Hand-build a request frame containing a reserved 'I' (traceId) record
        // between the known ones: a v1 decoder must ignore it.
        var body = new ByteArrayOutputStream();
        var bodyOut = new DataOutputStream(body);
        bodyOut.writeByte('T');
        byte[] type = "Cmd".getBytes(StandardCharsets.UTF_8);
        bodyOut.writeInt(type.length);
        bodyOut.write(type);
        bodyOut.writeByte('I');                       // unknown to the decoder
        bodyOut.writeInt(16);
        bodyOut.write(new byte[16]);
        bodyOut.writeByte('P');
        bodyOut.writeInt(2);
        bodyOut.write(new byte[]{1, 2});

        var frame = new ByteArrayOutputStream();
        var out = new DataOutputStream(frame);
        out.writeByte(WireCodec.KIND_COMMAND);
        out.writeByte(WireCodec.MODE_WAIT);
        out.writeLong(99L);
        out.writeInt(body.size());
        body.writeTo(out);

        var read = WireCodec.readRequest(new DataInputStream(new ByteArrayInputStream(frame.toByteArray())));
        assertEquals("Cmd", read.type());
        assertEquals(-1L, read.aggregateVersion(), "missing V record falls back to the -1 sentinel");
        assertArrayEquals(new byte[]{1, 2}, read.payload());
    }

    @Test
    void dripFedStreamStillDecodes() throws IOException {
        // One byte per read() call: readFully/readLong must assemble across
        // short reads exactly as on a slow socket.
        byte[] frame = writeRequest(new CommandRequest(WireCodec.MODE_WAIT, 7L, "Cmd", 3L, new byte[]{9, 9, 9}));
        var dripping = new InputStream() {
            private final ByteArrayInputStream inner = new ByteArrayInputStream(frame);

            @Override
            public int read() {
                return inner.read();
            }

            @Override
            public int read(byte[] b, int off, int len) {
                return inner.read(b, off, Math.min(len, 1));
            }
        };
        var read = WireCodec.readRequest(new DataInputStream(dripping));
        assertEquals(7L, read.requestId());
        assertArrayEquals(new byte[]{9, 9, 9}, read.payload());
    }

    @Test
    void truncatedFrameThrowsEof() throws IOException {
        byte[] frame = writeRequest(new CommandRequest(WireCodec.MODE_WAIT, 7L, "Cmd", 3L, new byte[]{9}));
        byte[] cut = java.util.Arrays.copyOf(frame, frame.length - 3);
        assertThrows(EOFException.class,
                () -> WireCodec.readRequest(new DataInputStream(new ByteArrayInputStream(cut))));
    }

    @Test
    void emptyStreamThrowsEof() {
        assertThrows(EOFException.class,
                () -> WireCodec.readRequest(new DataInputStream(new ByteArrayInputStream(new byte[0]))));
        assertThrows(EOFException.class,
                () -> WireCodec.readResponse(new DataInputStream(new ByteArrayInputStream(new byte[0]))));
    }

    @Test
    void corruptBodyLengthRejectedBeforeAllocation() {
        var frame = new ByteArrayOutputStream();
        var out = new DataOutputStream(frame);
        try {
            out.writeByte(WireCodec.KIND_COMMAND);
            out.writeByte(WireCodec.MODE_WAIT);
            out.writeLong(1L);
            out.writeInt(Integer.MAX_VALUE);          // corrupt length prefix
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        var ex = assertThrows(IOException.class,
                () -> WireCodec.readRequest(new DataInputStream(new ByteArrayInputStream(frame.toByteArray()))));
        assertTrue(ex.getMessage().contains("out of bounds"));
    }

    @Test
    void wrongKindRejected() throws IOException {
        byte[] frame = writeResponse(CommandResponse.ack(1L));
        assertThrows(IOException.class,
                () -> WireCodec.readRequest(new DataInputStream(new ByteArrayInputStream(frame))));
    }

    @Test
    void recordLengthBeyondBodyRejected() throws IOException {
        var frame = new ByteArrayOutputStream();
        var out = new DataOutputStream(frame);
        out.writeByte(WireCodec.KIND_COMMAND);
        out.writeByte(WireCodec.MODE_ACK);
        out.writeLong(2L);
        out.writeInt(5);                              // body: tag + len only
        out.writeByte('T');
        out.writeInt(1000);                           // claims more than remains
        var ex = assertThrows(IOException.class,
                () -> WireCodec.readRequest(new DataInputStream(new ByteArrayInputStream(frame.toByteArray()))));
        assertTrue(ex.getMessage().contains("out of bounds"));
    }
}
