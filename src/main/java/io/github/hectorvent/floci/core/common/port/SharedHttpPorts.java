package io.github.hectorvent.floci.core.common.port;

import io.github.hectorvent.floci.config.FlociCertificateAuthority;
import io.github.hectorvent.floci.services.acm.CertificateGenerator;
import io.github.hectorvent.floci.services.acm.model.KeyAlgorithm;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * One socket per port for every data plane serving HTTP on it: Vert.x round-robins two HTTP servers on
 * one port, so ELBv2 and Lattice both binding 443 would each answer half of the other's traffic.
 */
@ApplicationScoped
public class SharedHttpPorts {

    private static final Logger LOG = Logger.getLogger(SharedHttpPorts.class);
    private static final byte TLS_HANDSHAKE = 0x16;
    private static final String LOOPBACK = "127.0.0.1";

    public interface Tenant {

        /** {@code host} arrives lower-cased and without its port. */
        boolean servesHost(String host);

        void handle(HttpServerRequest request);
    }

    public record ServerCertificate(String certificateChainPem, String privateKeyPem) {}

    private record Registration(Tenant tenant, boolean fallback) {}

    /** A data plane that terminates its own connections, reached by relaying the raw bytes to it. */
    private record Backend(Object owner, int port) {}

    private static final class SharedPort {
        private final List<Registration> registrations = new CopyOnWriteArrayList<>();
        private volatile Backend backend;
        private NetServer front;
        private HttpServer plain;
        private HttpServer tls;
        private Future<HttpServer> tlsReady;
        private Future<Void> bound;
    }

    private final Vertx vertx;
    private final FlociCertificateAuthority certificateAuthority;
    private final Map<Integer, SharedPort> ports = new ConcurrentHashMap<>();
    private final Map<String, ServerCertificate> certificates = new ConcurrentHashMap<>();
    private volatile ServerCertificate defaultCertificate;
    private NetClient relayClient;

    @Inject
    public SharedHttpPorts(Vertx vertx, FlociCertificateAuthority certificateAuthority) {
        this.vertx = vertx;
        this.certificateAuthority = certificateAuthority;
    }

    /** A fallback tenant gets the requests whose Host no tenant serves, when the port has no backend. */
    public synchronized Future<Void> attach(int port, Tenant tenant, boolean fallback) {
        SharedPort shared = sharedPort(port);
        shared.registrations.add(new Registration(tenant, fallback));
        return shared.bound;
    }

    public synchronized void detach(int port, Tenant tenant) {
        SharedPort shared = ports.get(port);
        if (shared == null) {
            return;
        }
        shared.registrations.removeIf(registration -> registration.tenant() == tenant);
        closeIfUnused(port, shared);
    }

    /**
     * Relays every connection no tenant claims, TLS included, untouched to {@code backendPort} on
     * loopback, where the owner serves it as it would on the port itself. One backend per port.
     */
    public synchronized Future<Void> attachBackend(int port, Object owner, int backendPort) {
        SharedPort shared = sharedPort(port);
        shared.backend = new Backend(owner, backendPort);
        return shared.bound;
    }

    /** Completes once the port is free again when nothing else listens through it. */
    public synchronized Future<Void> detachBackend(int port, Object owner) {
        SharedPort shared = ports.get(port);
        if (shared == null || shared.backend == null || shared.backend.owner() != owner) {
            return Future.succeededFuture();
        }
        shared.backend = null;
        return closeIfUnused(port, shared);
    }

    private SharedPort sharedPort(int port) {
        SharedPort shared = ports.get(port);
        if (shared == null) {
            shared = new SharedPort();
            ports.put(port, shared);
            shared.bound = bind(port, shared);
        }
        return shared;
    }

    private Future<Void> closeIfUnused(int port, SharedPort shared) {
        if (shared.registrations.isEmpty() && shared.backend == null) {
            ports.remove(port, shared);
            return close(shared);
        }
        return Future.succeededFuture();
    }

    public void putCertificate(String key, ServerCertificate certificate) {
        if (!certificate.equals(certificates.put(key, certificate))) {
            reloadCertificates();
        }
    }

    public void removeCertificate(String key) {
        if (certificates.remove(key) != null) {
            reloadCertificates();
        }
    }

    @PreDestroy
    synchronized void shutdown() {
        ports.values().forEach(this::close);
        ports.clear();
        if (relayClient != null) {
            relayClient.close();
        }
    }

    private Future<Void> bind(int port, SharedPort shared) {
        Handler<HttpServerRequest> dispatcher = request -> dispatch(shared, request);
        shared.plain = vertx.createHttpServer(new HttpServerOptions().setHost(LOOPBACK).setPort(0));
        shared.plain.requestHandler(dispatcher);
        shared.front = vertx.createNetServer(new NetServerOptions().setHost("0.0.0.0").setPort(port));
        shared.front.connectHandler(socket -> relay(shared, socket));
        return shared.plain.listen()
                .compose(ignored -> shared.front.listen())
                .<Void>mapEmpty()
                .onSuccess(ignored -> LOG.infov("Shared HTTP port {0} started", String.valueOf(port)))
                .onFailure(err -> {
                    synchronized (this) {
                        ports.remove(port, shared);
                    }
                    close(shared);
                    LOG.warnv("Shared HTTP port {0} failed to start: {1}", String.valueOf(port), err.getMessage());
                });
    }

    /** Reads until the Host (SNI for TLS) is known, then relays the connection to whoever serves it. */
    private void relay(SharedPort shared, NetSocket front) {
        front.pause();
        Buffer head = Buffer.buffer();
        front.handler(chunk -> {
            head.appendBuffer(chunk);
            boolean tls = head.getByte(0) == TLS_HANDSHAKE;
            HostPeek.Result peek = tls ? HostPeek.serverName(head) : HostPeek.hostHeader(head);
            if (!peek.complete()) {
                return;
            }
            front.handler(null);
            front.pause();
            target(shared, tls, peek.host()).compose(port -> relayClient().connect(port, LOOPBACK)).onComplete(ar -> {
                if (ar.failed()) {
                    LOG.debugv("Shared HTTP port relay failed: {0}", ar.cause().getMessage());
                    front.close();
                    return;
                }
                NetSocket back = ar.result();
                back.write(head);
                front.pipeTo(back).onFailure(err ->
                        LOG.debugv("Shared HTTP port pipe front to back failed: {0}", err.getMessage()));
                back.pipeTo(front).onFailure(err ->
                        LOG.debugv("Shared HTTP port pipe back to front failed: {0}", err.getMessage()));
            });
        });
        front.resume();
    }

    /** The loopback port that serves a connection naming {@code host}, or null when nothing names it. */
    private Future<Integer> target(SharedPort shared, boolean tls, String host) {
        boolean claimed = host != null && shared.registrations.stream()
                .anyMatch(registration -> registration.tenant().servesHost(host));
        Backend backend = shared.backend;
        if (!claimed && backend != null) {
            return Future.succeededFuture(backend.port());
        }
        // With no certificate put, no tenant serves TLS, so a handshake reaches the plain server and fails.
        if (tls && !certificates.isEmpty()) {
            return tlsServer(shared).map(HttpServer::actualPort);
        }
        return Future.succeededFuture(shared.plain.actualPort());
    }

    /** Started on the first TLS connection, so a port nobody reaches over TLS never mints a key. */
    private synchronized Future<HttpServer> tlsServer(SharedPort shared) {
        if (shared.tlsReady == null) {
            shared.tlsReady = vertx.executeBlocking(this::keyCertOptions, false).compose(options -> {
                HttpServer server = vertx.createHttpServer(new HttpServerOptions().setHost(LOOPBACK).setPort(0)
                        .setSsl(true).setSni(true).setKeyCertOptions(options));
                server.requestHandler(request -> dispatch(shared, request));
                synchronized (this) {
                    shared.tls = server;
                }
                // Picks up a certificate put while the key was being minted.
                return server.listen().compose(listening -> listening.updateSSLOptions(
                        new SSLOptions().setKeyCertOptions(keyCertOptions())).map(listening));
            });
        }
        return shared.tlsReady;
    }

    private void dispatch(SharedPort shared, HttpServerRequest request) {
        String host = normalizeHost(request.host());
        for (Registration registration : shared.registrations) {
            if (registration.tenant().servesHost(host)) {
                registration.tenant().handle(request);
                return;
            }
        }
        for (Registration registration : shared.registrations) {
            if (registration.fallback()) {
                registration.tenant().handle(request);
                return;
            }
        }
        request.response().setStatusCode(404).end("No service for host");
    }

    private synchronized NetClient relayClient() {
        if (relayClient == null) {
            relayClient = vertx.createNetClient();
        }
        return relayClient;
    }

    private void reloadCertificates() {
        PemKeyCertOptions options = keyCertOptions();
        for (SharedPort shared : ports.values()) {
            if (shared.tls != null) {
                shared.tls.updateSSLOptions(new SSLOptions().setKeyCertOptions(options))
                        .onFailure(err -> LOG.warnv("Could not reload shared HTTP port certificates: {0}",
                                err.getMessage()));
            }
        }
    }

    /** The default certificate goes first: Vert.x serves the first one when SNI matches none. */
    private PemKeyCertOptions keyCertOptions() {
        List<ServerCertificate> all = new ArrayList<>();
        all.add(defaultCertificate());
        all.addAll(certificates.values());
        PemKeyCertOptions options = new PemKeyCertOptions();
        for (ServerCertificate certificate : all) {
            options.addCertValue(Buffer.buffer(certificate.certificateChainPem()));
            options.addKeyValue(Buffer.buffer(certificate.privateKeyPem()));
        }
        return options;
    }

    private ServerCertificate defaultCertificate() {
        if (defaultCertificate == null) {
            CertificateGenerator.GeneratedCertificate issued = certificateAuthority.issueServerCertificate(
                    "localhost", List.of("localhost"), KeyAlgorithm.RSA_2048, null);
            defaultCertificate = new ServerCertificate(issued.certificatePem() + certificateAuthority.caPem(),
                    issued.privateKeyPem());
        }
        return defaultCertificate;
    }

    private Future<Void> close(SharedPort shared) {
        List<Future<Void>> closing = new ArrayList<>();
        if (shared.front != null) {
            closing.add(shared.front.close());
        }
        if (shared.plain != null) {
            closing.add(shared.plain.close());
        }
        if (shared.tls != null) {
            closing.add(shared.tls.close());
        }
        return Future.join(closing).mapEmpty();
    }

    public static String normalizeHost(String host) {
        if (host == null) {
            return "";
        }
        String normalized = host.trim().toLowerCase(Locale.ROOT);
        int colon = normalized.lastIndexOf(':');
        int bracket = normalized.lastIndexOf(']');
        if (colon > -1 && colon > bracket) {
            normalized = normalized.substring(0, colon);
        }
        if (normalized.endsWith(".")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }
}
