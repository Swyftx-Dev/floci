package io.github.hectorvent.floci.services.msk;

import io.github.hectorvent.floci.config.FlociCertificateAuthority;
import io.github.hectorvent.floci.services.msk.model.ClientAuthentication;
import io.github.hectorvent.floci.services.msk.model.ClusterState;
import io.github.hectorvent.floci.services.msk.model.EncryptionInTransit;
import io.github.hectorvent.floci.services.msk.model.EncryptionInfo;
import io.github.hectorvent.floci.services.msk.model.Iam;
import io.github.hectorvent.floci.services.msk.model.MskCluster;
import io.github.hectorvent.floci.services.msk.model.Sasl;
import io.github.hectorvent.floci.testing.RealMskBrokerFrontEndProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.vertx.core.buffer.Buffer;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Drives the SASL/IAM listener with raw Kafka frames over TLS, in front of a fake broker that answers
 * every request with a Metadata v8 response naming itself.
 */
@QuarkusTest
@TestProfile(RealMskBrokerFrontEndProfile.class)
class MskBrokerFrontEndIntegrationTest {

    private static final String ARN =
            "arn:aws:kafka:ap-southeast-2:000000000000:cluster/frontend/1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d-3";
    private static final String BROKER_HOST = "b-1.frontend.1a2b3c.c1.kafka.ap-southeast-2.amazonaws.com";
    private static final int IAM_PORT = 9098;

    @Inject
    MskBrokerFrontEnd frontEnd;

    @Inject
    FlociCertificateAuthority certificateAuthority;

    private ServerSocket fakeBroker;

    @BeforeEach
    void startFakeBrokerAndListener() throws Exception {
        fakeBroker = new ServerSocket(0);
        Thread.ofVirtual().start(this::acceptBrokerConnections);
        awaitListening(cluster("127.0.0.1:" + fakeBroker.getLocalPort()));
    }

    @AfterEach
    void stop() throws IOException {
        frontEnd.sync(List.of());
        fakeBroker.close();
    }

    @Test
    void requestBeforeAuthenticationClosesTheConnection() throws Exception {
        try (SSLSocket socket = connect()) {
            send(socket, request(KafkaWire.METADATA, (short) 8, 1, metadataBody()));

            DataInputStream in = new DataInputStream(socket.getInputStream());
            assertThrows(IOException.class, in::readInt);
        }
    }

    @Test
    void iamAuthenticationThenMetadataAdvertisesTheListener() throws Exception {
        try (SSLSocket socket = connect()) {
            Buffer handshake = exchange(socket, request(KafkaWire.SASL_HANDSHAKE, (short) 1, 1,
                    string("AWS_MSK_IAM")));
            assertEquals(1, handshake.getInt(0));
            assertEquals(0, handshake.getShort(4));

            String payload = "{\"version\":\"2020_10_22\",\"host\":\"" + BROKER_HOST + "\","
                    + "\"action\":\"kafka-cluster:Connect\","
                    + "\"x-amz-credential\":\"AKIA/20261020/ap-southeast-2/kafka-cluster/aws4_request\","
                    + "\"x-amz-date\":\"20261020T000000Z\",\"x-amz-signature\":\"abc\"}";
            byte[] token = payload.getBytes(StandardCharsets.UTF_8);
            Buffer authenticate = exchange(socket, request(KafkaWire.SASL_AUTHENTICATE, (short) 1, 2,
                    Buffer.buffer().appendInt(token.length).appendBytes(token)));
            assertEquals(2, authenticate.getInt(0));
            assertEquals(0, authenticate.getShort(4));

            Buffer metadata = exchange(socket, request(KafkaWire.METADATA, (short) 8, 3, metadataBody()));
            assertEquals(3, metadata.getInt(0));
            assertEquals(BROKER_HOST, KafkaWire.readString(metadata, 16));
            assertEquals(IAM_PORT, metadata.getInt(16 + 2 + BROKER_HOST.length()));
        }
    }

    @Test
    void malformedIamPayloadIsRefused() throws Exception {
        try (SSLSocket socket = connect()) {
            exchange(socket, request(KafkaWire.SASL_HANDSHAKE, (short) 1, 1, string("AWS_MSK_IAM")));
            byte[] token = "{\"version\":\"2020_10_22\"}".getBytes(StandardCharsets.UTF_8);
            Buffer authenticate = exchange(socket, request(KafkaWire.SASL_AUTHENTICATE, (short) 1, 2,
                    Buffer.buffer().appendInt(token.length).appendBytes(token)));

            assertEquals(KafkaWire.SASL_AUTHENTICATION_FAILED, authenticate.getShort(4));
        }
    }

    private static MskCluster cluster(String bootstrapBrokers) {
        Iam iam = new Iam();
        iam.setEnabled(true);
        Sasl sasl = new Sasl();
        sasl.setIam(iam);
        ClientAuthentication auth = new ClientAuthentication();
        auth.setSasl(sasl);
        EncryptionInTransit inTransit = new EncryptionInTransit();
        inTransit.setClientBroker("TLS");
        EncryptionInfo encryption = new EncryptionInfo();
        encryption.setEncryptionInTransit(inTransit);
        MskCluster cluster = new MskCluster();
        cluster.setClusterArn(ARN);
        cluster.setClusterName("frontend");
        cluster.setNumberOfBrokerNodes(1);
        cluster.setClientAuthentication(auth);
        cluster.setEncryptionInfo(encryption);
        cluster.setState(ClusterState.ACTIVE);
        cluster.setBootstrapBrokers(bootstrapBrokers);
        return cluster;
    }

    private void acceptBrokerConnections() {
        while (!fakeBroker.isClosed()) {
            try {
                Socket connection = fakeBroker.accept();
                Thread.ofVirtual().start(() -> answerMetadata(connection));
            } catch (IOException expected) {
                // The test closed the server socket: nothing more to accept.
                return;
            }
        }
    }

    private void answerMetadata(Socket connection) {
        try (connection) {
            DataInputStream in = new DataInputStream(connection.getInputStream());
            DataOutputStream out = new DataOutputStream(connection.getOutputStream());
            while (true) {
                byte[] frame = new byte[in.readInt()];
                in.readFully(frame);
                int correlationId = Buffer.buffer(frame).getInt(4);
                Buffer body = Buffer.buffer().appendInt(correlationId).appendInt(0)
                        .appendInt(1).appendInt(0).appendBuffer(string("127.0.0.1"))
                        .appendInt(fakeBroker.getLocalPort()).appendShort((short) -1)
                        .appendBuffer(string("cluster-id")).appendInt(0).appendInt(0);
                out.write(KafkaWire.framed(body).getBytes());
                out.flush();
            }
        } catch (IOException expected) {
            // The front end closed its side: the exchange is over.
        }
    }

    /** Syncs until the port is open: the previous test's listener may still be releasing it. */
    private void awaitListening(MskCluster cluster) throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(15);
        while (Instant.now().isBefore(deadline)) {
            frontEnd.sync(List.of(cluster));
            try (Socket probe = new Socket("127.0.0.1", IAM_PORT)) {
                return;
            } catch (IOException notYet) {
                Thread.sleep(100);
            }
        }
        throw new IllegalStateException("The MSK IAM listener never opened port " + IAM_PORT);
    }

    private SSLSocket connect() throws Exception {
        KeyStore trust = KeyStore.getInstance(KeyStore.getDefaultType());
        trust.load(null, null);
        trust.setCertificateEntry("floci", certificateAuthority.certificate());
        TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        factory.init(trust);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, factory.getTrustManagers(), null);
        SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket("127.0.0.1", IAM_PORT);
        SSLParameters parameters = socket.getSSLParameters();
        parameters.setServerNames(List.of(new SNIHostName(BROKER_HOST)));
        socket.setSSLParameters(parameters);
        socket.setSoTimeout(10_000);
        return socket;
    }

    private static Buffer exchange(SSLSocket socket, Buffer request) throws IOException {
        send(socket, request);
        DataInputStream in = new DataInputStream(socket.getInputStream());
        try {
            byte[] response = new byte[in.readInt()];
            in.readFully(response);
            return Buffer.buffer(response);
        } catch (SocketTimeoutException e) {
            throw new IllegalStateException("No response from the MSK listener", e);
        }
    }

    private static void send(SSLSocket socket, Buffer request) throws IOException {
        socket.getOutputStream().write(request.getBytes());
        socket.getOutputStream().flush();
    }

    private static Buffer request(short apiKey, short apiVersion, int correlationId, Buffer body) {
        Buffer frame = Buffer.buffer().appendShort(apiKey).appendShort(apiVersion).appendInt(correlationId)
                .appendBuffer(string("test-client")).appendBuffer(body);
        return KafkaWire.framed(frame);
    }

    private static Buffer metadataBody() {
        return Buffer.buffer().appendInt(-1).appendByte((byte) 1).appendByte((byte) 0).appendByte((byte) 0);
    }

    private static Buffer string(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        return Buffer.buffer().appendShort((short) bytes.length).appendBytes(bytes);
    }
}
