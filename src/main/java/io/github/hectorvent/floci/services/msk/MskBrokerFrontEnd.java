package io.github.hectorvent.floci.services.msk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.FlociCertificateAuthority;
import io.github.hectorvent.floci.services.acm.CertificateGenerator;
import io.github.hectorvent.floci.services.acm.model.KeyAlgorithm;
import io.github.hectorvent.floci.services.msk.model.ClusterState;
import io.github.hectorvent.floci.services.msk.model.MskCluster;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.SSLOptions;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Serves each cluster's TLS and IAM listeners on Floci's address, as MSK's brokers do, in front of
 * the cluster's plaintext Redpanda broker: TLS is terminated with a Floci-CA certificate chosen by
 * SNI, SASL AWS_MSK_IAM and OAUTHBEARER are answered here, and broker addresses in responses are
 * rewritten to the listener the client came in on. Signatures are checked for shape, not verified.
 */
@ApplicationScoped
public class MskBrokerFrontEnd {

    private static final Logger LOG = Logger.getLogger(MskBrokerFrontEnd.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final List<String> MECHANISMS = List.of("AWS_MSK_IAM", "OAUTHBEARER");
    private static final int MAX_FRAME = 100 * 1024 * 1024;

    private final Vertx vertx;
    private final EmulatorConfig config;
    private final FlociCertificateAuthority certificateAuthority;
    private final Map<Integer, NetServer> servers = new ConcurrentHashMap<>();
    private final Map<String, CertificateGenerator.GeneratedCertificate> certificates = new ConcurrentHashMap<>();
    private volatile Map<String, MskBrokerEndpoints.Endpoint> endpointsByHost = Map.of();
    private volatile Map<String, MskCluster> clustersByArn = Map.of();
    private volatile Set<String> signature = Set.of();
    private NetClient upstreamClient;

    @Inject
    public MskBrokerFrontEnd(Vertx vertx, EmulatorConfig config, FlociCertificateAuthority certificateAuthority) {
        this.vertx = vertx;
        this.config = config;
        this.certificateAuthority = certificateAuthority;
    }

    /** Whether {@code host} is one of a cluster's broker names, which then resolves to Floci. */
    public boolean ownsHost(String host) {
        return endpointsByHost.containsKey(host.toLowerCase(Locale.ROOT));
    }

    /** Opens and closes listener ports and reissues certificates to match the clusters. */
    public synchronized void sync(List<MskCluster> clusters) {
        if (config.services().msk().mock()) {
            return;
        }
        Map<String, MskBrokerEndpoints.Endpoint> byHost = new HashMap<>();
        Map<String, MskCluster> byArn = new HashMap<>();
        Set<String> nextSignature = new HashSet<>();
        Set<Integer> ports = new HashSet<>();
        for (MskCluster cluster : clusters) {
            if (cluster.getState() != ClusterState.ACTIVE || cluster.getBootstrapBrokers() == null) {
                continue;
            }
            List<MskBrokerEndpoints.Endpoint> endpoints = MskBrokerEndpoints.allEndpoints(cluster);
            if (endpoints.isEmpty()) {
                continue;
            }
            byArn.put(cluster.getClusterArn(), cluster);
            for (MskBrokerEndpoints.Endpoint endpoint : endpoints) {
                byHost.put(endpoint.host(), endpoint);
                ports.add(endpoint.port());
                nextSignature.add(endpoint.address() + "|" + cluster.getBootstrapBrokers());
            }
        }
        clustersByArn = Map.copyOf(byArn);
        endpointsByHost = Map.copyOf(byHost);
        if (nextSignature.equals(signature)) {
            return;
        }
        signature = Set.copyOf(nextSignature);
        try {
            PemKeyCertOptions keyCerts = keyCertOptions(byArn.values());
            for (Integer port : ports) {
                if (!servers.containsKey(port)) {
                    listen(port, keyCerts);
                }
            }
            for (Integer port : Set.copyOf(servers.keySet())) {
                if (!ports.contains(port)) {
                    servers.remove(port).close();
                }
            }
            servers.values().forEach(server -> server.updateSSLOptions(new SSLOptions().setKeyCertOptions(keyCerts)));
        } catch (RuntimeException e) {
            LOG.warnv(e, "Could not bring the MSK broker listeners in line with the clusters: {0}", e.getMessage());
        }
    }

    @PreDestroy
    synchronized void shutdown() {
        servers.values().forEach(NetServer::close);
        servers.clear();
        if (upstreamClient != null) {
            upstreamClient.close();
        }
    }

    private void listen(int port, PemKeyCertOptions keyCerts) {
        NetServer server = vertx.createNetServer(new NetServerOptions().setHost("0.0.0.0").setPort(port)
                .setSsl(true).setSni(true).setKeyCertOptions(keyCerts));
        server.connectHandler(socket -> new Connection(socket, port).start());
        servers.put(port, server);
        server.listen()
                .onSuccess(ignored -> LOG.infov("MSK broker listener started on {0}", String.valueOf(port)))
                .onFailure(err -> {
                    servers.remove(port, server);
                    // Forgets the cluster set, so the next sync tries the port again.
                    signature = Set.of();
                    LOG.warnv("MSK broker listener port {0} failed to start: {1}", String.valueOf(port),
                            err.getMessage());
                });
    }

    /** The first certificate is served when SNI names no cluster. */
    private PemKeyCertOptions keyCertOptions(Iterable<MskCluster> clusters) {
        PemKeyCertOptions options = new PemKeyCertOptions();
        Set<String> live = new HashSet<>();
        for (MskCluster cluster : clusters) {
            List<String> hosts = MskBrokerEndpoints.allEndpoints(cluster).stream()
                    .map(MskBrokerEndpoints.Endpoint::host).toList();
            String key = cluster.getClusterArn() + "|" + String.join(",", hosts);
            live.add(key);
            CertificateGenerator.GeneratedCertificate issued = certificates.computeIfAbsent(key, ignored ->
                    certificateAuthority.issueServerCertificate(MskBrokerEndpoints.domain(cluster).split("\\.")[0],
                            hosts, KeyAlgorithm.RSA_2048, null));
            options.addCertValue(Buffer.buffer(issued.certificatePem() + certificateAuthority.caPem()));
            options.addKeyValue(Buffer.buffer(issued.privateKeyPem()));
        }
        certificates.keySet().retainAll(live);
        return options;
    }

    private synchronized NetClient upstreamClient() {
        if (upstreamClient == null) {
            upstreamClient = vertx.createNetClient();
        }
        return upstreamClient;
    }

    private record Pending(short apiKey, short apiVersion) {}

    /** One client connection and its connection to the cluster's broker. */
    private final class Connection {

        private final NetSocket client;
        private final int port;
        private final Deque<Pending> pending = new ArrayDeque<>();
        private final List<Buffer> queued = new ArrayList<>();
        private Buffer clientBuffer = Buffer.buffer();
        private Buffer upstreamBuffer = Buffer.buffer();
        private MskBrokerEndpoints.Endpoint endpoint;
        private MskBrokerEndpoints.Endpoint advertised;
        private NetSocket upstream;
        private boolean authenticated;
        private boolean rawSaslNext;
        private boolean closed;

        private Connection(NetSocket client, int port) {
            this.client = client;
            this.port = port;
        }

        private void start() {
            endpoint = resolveEndpoint(client.indicatedServerName());
            if (endpoint == null) {
                LOG.debugv("MSK listener {0}: no cluster for SNI {1}", String.valueOf(port), client.indicatedServerName());
                client.close();
                return;
            }
            // Read once: a sync while the connection is open must not change where it leads.
            MskCluster cluster = clustersByArn.get(endpoint.clusterArn());
            if (cluster == null || cluster.getBootstrapBrokers() == null) {
                client.close();
                return;
            }
            advertised = MskBrokerEndpoints.endpoints(cluster, endpoint.family()).getFirst();
            authenticated = !endpoint.family().requiresSasl();
            client.handler(this::onClientData);
            client.closeHandler(ignored -> close());
            client.exceptionHandler(err -> close());
            String[] upstreamAddress = cluster.getBootstrapBrokers().split(",")[0].split(":");
            client.pause();
            upstreamClient().connect(Integer.parseInt(upstreamAddress[1]), upstreamAddress[0]).onComplete(ar -> {
                if (ar.failed()) {
                    LOG.warnv("MSK listener could not reach the broker of {0}: {1}", endpoint.clusterArn(),
                            ar.cause().getMessage());
                    close();
                    return;
                }
                upstream = ar.result();
                if (closed) {
                    upstream.close();
                    return;
                }
                upstream.handler(this::onUpstreamData);
                upstream.closeHandler(ignored -> close());
                upstream.exceptionHandler(err -> close());
                queued.forEach(upstream::write);
                queued.clear();
                client.drainHandler(ignored -> upstream.resume());
                upstream.drainHandler(ignored -> client.resume());
                client.resume();
            });
        }

        private MskBrokerEndpoints.Endpoint resolveEndpoint(String serverName) {
            if (serverName != null) {
                MskBrokerEndpoints.Endpoint named = endpointsByHost.get(serverName.toLowerCase(Locale.ROOT));
                if (named != null && named.port() == port) {
                    return named;
                }
            }
            return endpointsByHost.values().stream().filter(e -> e.port() == port).findFirst().orElse(null);
        }

        private void onClientData(Buffer data) {
            try {
                readClientFrames(data);
            } catch (RuntimeException e) {
                LOG.debugv("MSK listener {0}: closing on a malformed request: {1}", endpoint.host(), e.getMessage());
                close();
            }
        }

        private void readClientFrames(Buffer data) {
            clientBuffer = clientBuffer.appendBuffer(data);
            while (clientBuffer.length() >= 4) {
                int size = clientBuffer.getInt(0);
                if (size < 0 || size > MAX_FRAME) {
                    close();
                    return;
                }
                if (clientBuffer.length() < 4 + size) {
                    return;
                }
                Buffer frame = clientBuffer.getBuffer(4, 4 + size);
                clientBuffer = clientBuffer.getBuffer(4 + size, clientBuffer.length());
                onRequest(frame);
                if (closed) {
                    return;
                }
            }
        }

        private void onRequest(Buffer frame) {
            if (rawSaslNext) {
                rawSaslNext = false;
                byte[] response = authenticate(frame.getBytes());
                if (response == null) {
                    close();
                    return;
                }
                client.write(KafkaWire.rawSaslResponse(response));
                return;
            }
            KafkaWire.RequestHeader header = KafkaWire.requestHeader(frame);
            switch (header.apiKey()) {
                case KafkaWire.SASL_HANDSHAKE -> {
                    String mechanism = KafkaWire.readString(frame, header.bodyOffset());
                    boolean supported = MECHANISMS.contains(mechanism);
                    rawSaslNext = supported && header.apiVersion() == 0;
                    client.write(KafkaWire.saslHandshakeResponse(header.correlationId(),
                            supported ? KafkaWire.NONE : KafkaWire.UNSUPPORTED_SASL_MECHANISM, MECHANISMS));
                }
                case KafkaWire.SASL_AUTHENTICATE -> {
                    byte[] response = authenticate(KafkaWire.readBytes(frame, header.bodyOffset()));
                    client.write(KafkaWire.saslAuthenticateResponse(header.correlationId(), header.apiVersion(),
                            response != null ? KafkaWire.NONE : KafkaWire.SASL_AUTHENTICATION_FAILED,
                            response != null ? null : "Access denied", response != null ? response : new byte[0]));
                }
                default -> {
                    if (!authenticated && header.apiKey() != KafkaWire.API_VERSIONS) {
                        LOG.debugv("MSK listener {0}: request {1} before authentication", endpoint.host(),
                                String.valueOf(header.apiKey()));
                        close();
                        return;
                    }
                    pending.addLast(new Pending(header.apiKey(), header.apiVersion()));
                    Buffer framed = KafkaWire.framed(frame);
                    if (upstream == null) {
                        queued.add(framed);
                    } else {
                        upstream.write(framed);
                        if (upstream.writeQueueFull()) {
                            client.pause();
                        }
                    }
                }
            }
        }

        /** The bytes to answer a well-formed token with, or null to refuse it. */
        private byte[] authenticate(byte[] token) {
            String text = new String(token, StandardCharsets.UTF_8);
            if (text.startsWith("n,") && text.contains("auth=Bearer ")) {
                String bearer = text.substring(text.indexOf("auth=Bearer ") + "auth=Bearer ".length()).split("\u0001")[0];
                authenticated = !bearer.isBlank();
                return authenticated ? new byte[0] : null;
            }
            try {
                JsonNode payload = JSON.readTree(text);
                authenticated = "2020_10_22".equals(payload.path("version").asText())
                        && "kafka-cluster:Connect".equals(payload.path("action").asText())
                        && payload.path("x-amz-credential").asText().contains("/kafka-cluster/aws4_request")
                        && !payload.path("x-amz-signature").asText().isBlank()
                        && !payload.path("x-amz-date").asText().isBlank();
            } catch (JsonProcessingException e) {
                LOG.debugv("Rejecting an AWS_MSK_IAM payload that is not JSON: {0}", e.getMessage());
                authenticated = false;
            }
            if (!authenticated) {
                LOG.debugv("MSK listener {0}: refused a malformed AWS_MSK_IAM payload", endpoint.host());
                return null;
            }
            return ("{\"version\":\"2020_10_22\",\"request-id\":\"" + UUID.randomUUID() + "\"}")
                    .getBytes(StandardCharsets.UTF_8);
        }

        private void onUpstreamData(Buffer data) {
            try {
                readUpstreamFrames(data);
            } catch (RuntimeException e) {
                LOG.debugv("MSK listener {0}: closing on a malformed broker response: {1}", endpoint.host(),
                        e.getMessage());
                close();
            }
        }

        private void readUpstreamFrames(Buffer data) {
            upstreamBuffer = upstreamBuffer.appendBuffer(data);
            while (upstreamBuffer.length() >= 4) {
                int size = upstreamBuffer.getInt(0);
                if (size < 0 || size > MAX_FRAME) {
                    close();
                    return;
                }
                if (upstreamBuffer.length() < 4 + size) {
                    return;
                }
                Buffer frame = upstreamBuffer.getBuffer(4, 4 + size);
                upstreamBuffer = upstreamBuffer.getBuffer(4 + size, upstreamBuffer.length());
                Pending request = pending.pollFirst();
                client.write(request == null ? KafkaWire.framed(frame) : rewrite(request, frame));
                if (client.writeQueueFull()) {
                    upstream.pause();
                }
            }
        }

        private Buffer rewrite(Pending request, Buffer frame) {
            try {
                return switch (request.apiKey()) {
                    case KafkaWire.API_VERSIONS -> {
                        KafkaWire.capApiVersions(frame, request.apiVersion());
                        yield KafkaWire.framed(frame);
                    }
                    case KafkaWire.METADATA -> KafkaWire.rewriteMetadata(frame, request.apiVersion(),
                            advertised.host(), advertised.port());
                    case KafkaWire.FIND_COORDINATOR -> KafkaWire.rewriteFindCoordinator(frame, request.apiVersion(),
                            advertised.host(), advertised.port());
                    default -> KafkaWire.framed(frame);
                };
            } catch (RuntimeException e) {
                LOG.warnv("MSK listener could not rewrite a response to API {0} v{1}: {2}",
                        String.valueOf(request.apiKey()), String.valueOf(request.apiVersion()), e.getMessage());
                return KafkaWire.framed(frame);
            }
        }

        private void close() {
            if (closed) {
                return;
            }
            closed = true;
            client.close();
            if (upstream != null) {
                upstream.close();
            }
        }
    }
}
