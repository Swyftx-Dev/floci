package io.github.hectorvent.floci.core.common.port;

import io.vertx.core.buffer.Buffer;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HostPeekTest {

    @Test
    void hostHeaderIsReadOnceTheHeadersEnd() {
        Buffer partial = Buffer.buffer("GET / HTTP/1.1\r\nHost: Echo.Example.Internal:8080\r\n");
        assertFalse(HostPeek.hostHeader(partial).complete());

        HostPeek.Result result = HostPeek.hostHeader(partial.copy().appendString("Accept: */*\r\n\r\n"));
        assertTrue(result.complete());
        assertEquals("echo.example.internal", result.host());
    }

    @Test
    void requestWithoutHostIsCompleteWithNoHost() {
        HostPeek.Result result = HostPeek.hostHeader(Buffer.buffer("GET / HTTP/1.0\r\n\r\n"));
        assertTrue(result.complete());
        assertNull(result.host());
    }

    @Test
    void headersPastTheLimitStopTheWait() {
        Buffer endless = Buffer.buffer("GET / HTTP/1.1\r\nX-Pad: " + "a".repeat(HostPeek.MAX_PEEK));
        assertTrue(HostPeek.hostHeader(endless).complete());
    }

    @Test
    void serverNameIsReadFromAClientHello() throws Exception {
        Buffer hello = clientHello("svc-0123.abc.vpc-lattice-svcs.us-east-1.localhost.floci.io");

        assertFalse(HostPeek.serverName(hello.getBuffer(0, 20)).complete());
        HostPeek.Result result = HostPeek.serverName(hello);
        assertTrue(result.complete());
        assertEquals("svc-0123.abc.vpc-lattice-svcs.us-east-1.localhost.floci.io", result.host());
    }

    @Test
    void clientHelloWithoutServerNameIsCompleteWithNoHost() throws Exception {
        HostPeek.Result result = HostPeek.serverName(clientHello(null));
        assertTrue(result.complete());
        assertNull(result.host());
    }

    @Test
    void handshakeThatIsNotAClientHelloIsCompleteWithNoHost() {
        Buffer alert = Buffer.buffer().appendByte((byte) 0x16).appendShort((short) 0x0303).appendShort((short) 2)
                .appendByte((byte) 2).appendByte((byte) 0);
        assertNull(HostPeek.serverName(alert).host());
        assertTrue(HostPeek.serverName(alert).complete());
    }

    private static Buffer clientHello(String serverName) throws Exception {
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, null, null);
        SSLEngine engine = context.createSSLEngine();
        engine.setUseClientMode(true);
        SSLParameters parameters = engine.getSSLParameters();
        parameters.setServerNames(serverName == null ? List.of() : List.of(new SNIHostName(serverName)));
        engine.setSSLParameters(parameters);
        ByteBuffer out = ByteBuffer.allocate(engine.getSession().getPacketBufferSize());
        engine.wrap(ByteBuffer.allocate(0), out);
        out.flip();
        byte[] bytes = new byte[out.remaining()];
        out.get(bytes);
        return Buffer.buffer(bytes);
    }
}
