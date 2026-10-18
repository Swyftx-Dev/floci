package io.github.hectorvent.floci.core.common.port;

import io.vertx.core.buffer.Buffer;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * Reads the host a connection is for from its first bytes without consuming them: the SNI of a TLS
 * ClientHello, or the Host header of an HTTP/1 request. A connection that does not name one within
 * {@link #MAX_PEEK} bytes is complete with no host, so it still reaches a default.
 */
final class HostPeek {

    static final int MAX_PEEK = 16 * 1024;

    private static final int CLIENT_HELLO = 1;
    private static final int SERVER_NAME_EXTENSION = 0;
    private static final int HOST_NAME = 0;

    record Result(boolean complete, String host) {

        static final Result MORE = new Result(false, null);
        static final Result NONE = new Result(true, null);
    }

    private HostPeek() {
    }

    static Result hostHeader(Buffer head) {
        String text = head.getString(0, Math.min(head.length(), MAX_PEEK), StandardCharsets.ISO_8859_1.name());
        int end = text.indexOf("\r\n\r\n");
        if (end < 0) {
            return head.length() >= MAX_PEEK ? Result.NONE : Result.MORE;
        }
        for (String line : text.substring(0, end).split("\r\n")) {
            int colon = line.indexOf(':');
            if (colon > 0 && line.substring(0, colon).trim().equalsIgnoreCase("host")) {
                return new Result(true, SharedHttpPorts.normalizeHost(line.substring(colon + 1)));
            }
        }
        return Result.NONE;
    }

    /** The SNI host name of a ClientHello in the first TLS record. */
    static Result serverName(Buffer head) {
        if (head.length() < 5) {
            return Result.MORE;
        }
        int recordEnd = 5 + head.getUnsignedShort(3);
        if (head.length() < recordEnd) {
            return recordEnd > MAX_PEEK + 5 ? Result.NONE : Result.MORE;
        }
        try {
            int offset = 5;
            if (head.getUnsignedByte(offset) != CLIENT_HELLO) {
                return Result.NONE;
            }
            offset += 4 + 2 + 32;
            offset += 1 + head.getUnsignedByte(offset);
            offset += 2 + head.getUnsignedShort(offset);
            offset += 1 + head.getUnsignedByte(offset);
            int extensionsEnd = Math.min(offset + 2 + head.getUnsignedShort(offset), recordEnd);
            offset += 2;
            while (offset + 4 <= extensionsEnd) {
                int type = head.getUnsignedShort(offset);
                int length = head.getUnsignedShort(offset + 2);
                offset += 4;
                if (type == SERVER_NAME_EXTENSION) {
                    return new Result(true, hostName(head, offset, offset + length));
                }
                offset += length;
            }
            return Result.NONE;
        } catch (IndexOutOfBoundsException malformed) {
            // A ClientHello cut short or mis-sized names no host; the default server answers it.
            return Result.NONE;
        }
    }

    private static String hostName(Buffer head, int offset, int end) {
        int position = offset + 2;
        while (position + 3 <= end) {
            int type = head.getUnsignedByte(position);
            int length = head.getUnsignedShort(position + 1);
            position += 3;
            if (type == HOST_NAME) {
                return head.getString(position, position + length, StandardCharsets.US_ASCII.name())
                        .toLowerCase(Locale.ROOT);
            }
            position += length;
        }
        return null;
    }
}
