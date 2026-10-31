package io.github.hectorvent.floci.services.msk;

import io.vertx.core.buffer.Buffer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaWireTest {

    private static Buffer string(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        return Buffer.buffer().appendShort((short) bytes.length).appendBytes(bytes);
    }

    @Test
    void metadataBrokersAreRewrittenAndTheRestKept() {
        Buffer response = Buffer.buffer().appendInt(7).appendInt(0)
                .appendInt(1).appendInt(0).appendBuffer(string("redpanda:9092")).appendInt(9092).appendShort((short) -1)
                .appendBuffer(string("cluster-id")).appendInt(0).appendInt(0);

        Buffer framed = KafkaWire.rewriteMetadata(response, (short) 8, "b-1.iam.example.com", 14001);
        Buffer frame = framed.getBuffer(4, framed.length());

        assertEquals(frame.length(), framed.getInt(0));
        assertEquals(7, frame.getInt(0));
        assertEquals(1, frame.getInt(8));
        assertEquals("b-1.iam.example.com", KafkaWire.readString(frame, 16));
        assertEquals(14001, frame.getInt(16 + 2 + 19));
        assertEquals("cluster-id", KafkaWire.readString(frame, 16 + 2 + 19 + 4 + 2));
    }

    @Test
    void findCoordinatorAddressIsRewritten() {
        Buffer response = Buffer.buffer().appendInt(3).appendInt(0).appendShort((short) 0).appendShort((short) -1)
                .appendInt(0).appendBuffer(string("redpanda:9092")).appendInt(9092);

        Buffer frame = KafkaWire.rewriteFindCoordinator(response, (short) 2, "b-1.example.com", 9098);
        Buffer body = frame.getBuffer(4, frame.length());

        assertEquals("b-1.example.com", KafkaWire.readString(body, 16));
        assertEquals(9098, body.getInt(16 + 2 + 15));
    }

    @Test
    void apiVersionsAreCappedInPlace() {
        Buffer response = Buffer.buffer().appendInt(1).appendShort((short) 0).appendInt(2)
                .appendShort(KafkaWire.METADATA).appendShort((short) 0).appendShort((short) 12)
                .appendShort((short) 0).appendShort((short) 0).appendShort((short) 9)
                .appendInt(0);

        KafkaWire.capApiVersions(response, (short) 2);

        assertEquals(8, response.getShort(10 + 4));
        assertEquals(9, response.getShort(16 + 4));
    }

    @Test
    void saslAuthenticateReplyCarriesTheTokenAndSessionLifetime() {
        Buffer framed = KafkaWire.saslAuthenticateResponse(42, (short) 1, KafkaWire.NONE, null,
                "{}".getBytes(StandardCharsets.UTF_8));
        Buffer body = framed.getBuffer(4, framed.length());

        assertEquals(42, body.getInt(0));
        assertEquals(0, body.getShort(4));
        assertEquals(-1, body.getShort(6));
        assertEquals(2, body.getInt(8));
        assertEquals(0L, body.getLong(14));
    }

    @Test
    void saslHandshakeReplyListsTheMechanisms() {
        Buffer framed = KafkaWire.saslHandshakeResponse(5, KafkaWire.NONE, List.of("AWS_MSK_IAM", "OAUTHBEARER"));
        Buffer body = framed.getBuffer(4, framed.length());

        assertEquals(2, body.getInt(6));
        assertEquals("AWS_MSK_IAM", KafkaWire.readString(body, 10));
    }
}
