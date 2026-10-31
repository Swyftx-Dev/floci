package io.github.hectorvent.floci.services.msk;

import io.vertx.core.buffer.Buffer;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * The few Kafka wire messages the MSK broker front end reads or writes itself. Offsets are into a
 * frame without its 4-byte size prefix. Only non-flexible versions are parsed: the front end caps
 * the versions it advertises so clients never pick a flexible one of these.
 */
final class KafkaWire {

    static final short METADATA = 3;
    static final short FIND_COORDINATOR = 10;
    static final short SASL_HANDSHAKE = 17;
    static final short API_VERSIONS = 18;
    static final short SASL_AUTHENTICATE = 36;

    /** The highest version of each API the front end lets a client use. */
    static final Map<Short, Short> MAX_VERSIONS = Map.of(
            METADATA, (short) 8, FIND_COORDINATOR, (short) 3, SASL_HANDSHAKE, (short) 1, SASL_AUTHENTICATE, (short) 1);

    static final short NONE = 0;
    static final short ILLEGAL_SASL_STATE = 34;
    static final short UNSUPPORTED_SASL_MECHANISM = 33;
    static final short SASL_AUTHENTICATION_FAILED = 58;

    private KafkaWire() {
    }

    record RequestHeader(short apiKey, short apiVersion, int correlationId, int bodyOffset) {}

    static RequestHeader requestHeader(Buffer frame) {
        short clientIdLength = frame.getShort(8);
        int bodyOffset = 10 + Math.max(0, clientIdLength);
        return new RequestHeader(frame.getShort(0), frame.getShort(2), frame.getInt(4), bodyOffset);
    }

    static String readString(Buffer frame, int offset) {
        short length = frame.getShort(offset);
        return length < 0 ? null : frame.getString(offset + 2, offset + 2 + length, StandardCharsets.UTF_8.name());
    }

    static byte[] readBytes(Buffer frame, int offset) {
        int length = frame.getInt(offset);
        return length < 0 ? new byte[0] : frame.getBytes(offset + 4, offset + 4 + length);
    }

    static Buffer saslHandshakeResponse(int correlationId, short errorCode, List<String> mechanisms) {
        Buffer body = Buffer.buffer().appendInt(correlationId).appendShort(errorCode).appendInt(mechanisms.size());
        mechanisms.forEach(mechanism -> appendString(body, mechanism));
        return framed(body);
    }

    static Buffer saslAuthenticateResponse(int correlationId, short version, short errorCode, String errorMessage,
                                           byte[] authBytes) {
        Buffer body = Buffer.buffer().appendInt(correlationId).appendShort(errorCode);
        appendString(body, errorMessage);
        body.appendInt(authBytes.length).appendBytes(authBytes);
        if (version >= 1) {
            body.appendLong(0L);
        }
        return framed(body);
    }

    /** The reply to a SASL token sent after a v0 handshake: the token alone, size-prefixed. */
    static Buffer rawSaslResponse(byte[] authBytes) {
        return framed(Buffer.buffer().appendBytes(authBytes));
    }

    /** Lowers the max version of every capped API in place; the size never changes. */
    static void capApiVersions(Buffer response, short version) {
        int offset = 4 + 2;
        int count;
        if (version >= 3) {
            long[] varint = readUnsignedVarint(response, offset);
            count = (int) varint[0] - 1;
            offset = (int) varint[1];
        } else {
            count = response.getInt(offset);
            offset += 4;
        }
        for (int i = 0; i < count; i++) {
            short apiKey = response.getShort(offset);
            Short cap = MAX_VERSIONS.get(apiKey);
            if (cap != null && response.getShort(offset + 4) > cap) {
                response.setShort(offset + 4, cap);
            }
            offset += 6;
            if (version >= 3) {
                offset = skipTaggedFields(response, offset);
            }
        }
    }

    /** Replaces the broker list of a Metadata response (v0-8) with the one front-end address. */
    static Buffer rewriteMetadata(Buffer response, short version, String host, int port) {
        int offset = 4 + (version >= 3 ? 4 : 0);
        int count = response.getInt(offset);
        int brokersStart = offset;
        offset += 4;
        Buffer brokers = Buffer.buffer().appendInt(count);
        for (int i = 0; i < count; i++) {
            int nodeId = response.getInt(offset);
            offset += 4;
            offset += 2 + Math.max(0, response.getShort(offset));
            offset += 4;
            brokers.appendInt(nodeId);
            appendString(brokers, host);
            brokers.appendInt(port);
            if (version >= 1) {
                short rackLength = response.getShort(offset);
                brokers.appendBuffer(response.getBuffer(offset, offset + 2 + Math.max(0, rackLength)));
                offset += 2 + Math.max(0, rackLength);
            }
        }
        return framed(Buffer.buffer().appendBuffer(response.getBuffer(0, brokersStart)).appendBuffer(brokers)
                .appendBuffer(response.getBuffer(offset, response.length())));
    }

    /** Replaces the coordinator's address in a FindCoordinator response (v0-3). */
    static Buffer rewriteFindCoordinator(Buffer response, short version, String host, int port) {
        int offset = 4;
        if (version >= 1) {
            offset += 4 + 2;
            offset += 2 + Math.max(0, response.getShort(offset));
        } else {
            offset += 2;
        }
        int nodeId = response.getInt(offset);
        if (nodeId < 0) {
            return framed(response);
        }
        int hostOffset = offset + 4;
        int afterPort = hostOffset + 2 + Math.max(0, response.getShort(hostOffset)) + 4;
        Buffer address = Buffer.buffer();
        appendString(address, host);
        address.appendInt(port);
        return framed(Buffer.buffer().appendBuffer(response.getBuffer(0, hostOffset)).appendBuffer(address)
                .appendBuffer(response.getBuffer(afterPort, response.length())));
    }

    static Buffer framed(Buffer body) {
        return Buffer.buffer(body.length() + 4).appendInt(body.length()).appendBuffer(body);
    }

    private static void appendString(Buffer buffer, String value) {
        if (value == null) {
            buffer.appendShort((short) -1);
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        buffer.appendShort((short) bytes.length).appendBytes(bytes);
    }

    private static int skipTaggedFields(Buffer buffer, int offset) {
        long[] fields = readUnsignedVarint(buffer, offset);
        offset = (int) fields[1];
        for (long i = 0; i < fields[0]; i++) {
            offset = (int) readUnsignedVarint(buffer, offset)[1];
            long[] size = readUnsignedVarint(buffer, offset);
            offset = (int) size[1] + (int) size[0];
        }
        return offset;
    }

    /** The value and the offset just past it. */
    private static long[] readUnsignedVarint(Buffer buffer, int offset) {
        long value = 0;
        int shift = 0;
        while (true) {
            byte b = buffer.getByte(offset++);
            value |= (long) (b & 0x7f) << shift;
            if ((b & 0x80) == 0) {
                return new long[] {value, offset};
            }
            shift += 7;
        }
    }
}
