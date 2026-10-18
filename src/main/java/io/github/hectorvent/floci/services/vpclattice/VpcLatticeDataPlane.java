package io.github.hectorvent.floci.services.vpclattice;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.config.FlociCertificateAuthority;
import io.github.hectorvent.floci.config.TlsProxyServer;
import io.github.hectorvent.floci.core.common.RequestScopes;
import io.github.hectorvent.floci.core.common.SsrfProtection;
import io.github.hectorvent.floci.core.common.port.SharedHttpPorts;
import io.github.hectorvent.floci.services.acm.AcmService;
import io.github.hectorvent.floci.services.acm.CertificateGenerator;
import io.github.hectorvent.floci.services.acm.model.Certificate;
import io.github.hectorvent.floci.services.acm.model.KeyAlgorithm;
import io.github.hectorvent.floci.services.vpclattice.model.LatticeService;
import io.github.hectorvent.floci.services.vpclattice.model.Listener;
import io.github.hectorvent.floci.services.vpclattice.model.RuleAction;
import io.github.hectorvent.floci.services.vpclattice.model.ServiceNetwork;
import io.github.hectorvent.floci.services.vpclattice.model.ServiceNetworkServiceAssociation;
import io.github.hectorvent.floci.services.vpclattice.model.Target;
import io.github.hectorvent.floci.services.vpclattice.model.TargetGroup;
import io.github.hectorvent.floci.services.vpclattice.model.WeightedTargetGroup;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.RequestOptions;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** Serves every Lattice listener on Floci's own address, picking the service by Host header. */
@ApplicationScoped
public class VpcLatticeDataPlane {

    private static final Logger LOG = Logger.getLogger(VpcLatticeDataPlane.class);
    private static final Set<String> HOP_BY_HOP_HEADERS = Set.of("connection", "keep-alive", "transfer-encoding",
            "upgrade", "te", "trailers", "proxy-authorization", "proxy-authenticate");
    private static final String CERTIFICATE_KEY_PREFIX = "vpc-lattice:";

    private record Route(String accountId, String serviceId) {}

    private final Vertx vertx;
    private final EmulatorConfig config;
    private final SharedHttpPorts sharedHttpPorts;
    private final TlsProxyServer tlsProxyServer;
    private final FlociCertificateAuthority certificateAuthority;
    private final AcmService acmService;
    private final VpcLatticeHealthChecker healthChecker;
    private final Instance<VpcLatticeService> service;

    private final Map<Integer, SharedHttpPorts.Tenant> tenants = new ConcurrentHashMap<>();
    private final Map<String, SharedHttpPorts.ServerCertificate> generatedCertificates = new ConcurrentHashMap<>();
    private final Set<String> publishedCertificates = ConcurrentHashMap.newKeySet();
    private final Set<Integer> warnedReservedPorts = ConcurrentHashMap.newKeySet();
    private final Map<String, AtomicInteger> roundRobin = new ConcurrentHashMap<>();
    private volatile Map<String, Route> routes = Map.of();
    private HttpClient proxyClient;

    @Inject
    public VpcLatticeDataPlane(Vertx vertx, EmulatorConfig config, SharedHttpPorts sharedHttpPorts,
                               TlsProxyServer tlsProxyServer, FlociCertificateAuthority certificateAuthority,
                               AcmService acmService, VpcLatticeHealthChecker healthChecker,
                               Instance<VpcLatticeService> service) {
        this.vertx = vertx;
        this.config = config;
        this.sharedHttpPorts = sharedHttpPorts;
        this.tlsProxyServer = tlsProxyServer;
        this.certificateAuthority = certificateAuthority;
        this.acmService = acmService;
        this.healthChecker = healthChecker;
        this.service = service;
    }

    /** Never throws: a data plane failure must not fail the control plane call that triggered it. */
    public synchronized void refresh() {
        if (config.services().vpclattice().mock()) {
            return;
        }
        try {
            reconcile();
        } catch (RuntimeException e) {
            LOG.warnv(e, "Could not bring the VPC Lattice data plane in line with the control plane: {0}",
                    e.getMessage());
        }
    }

    private void reconcile() {
        VpcLatticeService control = service.get();
        List<LatticeService> services = control.allServices();
        List<Listener> listeners = control.allListeners();

        Map<String, Route> nextRoutes = new HashMap<>();
        for (LatticeService svc : services) {
            Route route = new Route(svc.getAccountId(), svc.getId());
            if (svc.getDnsEntry() != null && svc.getDnsEntry().getDomainName() != null) {
                nextRoutes.put(svc.getDnsEntry().getDomainName().toLowerCase(Locale.ROOT), route);
            }
            if (svc.getCustomDomainName() != null) {
                nextRoutes.put(svc.getCustomDomainName().toLowerCase(Locale.ROOT), route);
            }
        }
        routes = Map.copyOf(nextRoutes);

        Set<Integer> ports = new HashSet<>();
        for (Listener listener : listeners) {
            if (!"TLS_PASSTHROUGH".equals(listener.getProtocol()) && listener.getPort() != null) {
                ports.add(listener.getPort());
            }
        }
        for (Integer port : ports) {
            if (!tenants.containsKey(port)) {
                attach(port);
            }
        }
        for (Integer port : Set.copyOf(tenants.keySet())) {
            if (!ports.contains(port)) {
                sharedHttpPorts.detach(port, tenants.remove(port));
            }
        }

        refreshCertificates(services, listeners);
        List<TargetGroup> targetGroups = control.allTargetGroups();
        roundRobin.keySet().retainAll(targetGroups.stream().map(TargetGroup::getArn).collect(Collectors.toSet()));
        healthChecker.sync(targetGroups);
    }

    public VpcLatticeHealthChecker.Health health(TargetGroup tg, Target target) {
        return healthChecker.health(tg, target);
    }

    public static Optional<String> flociAddress() {
        try {
            return Optional.of(InetAddress.getLocalHost().getHostAddress());
        } catch (Exception e) {
            LOG.debugv("Could not determine Floci's address: {0}", e.getMessage());
            return Optional.empty();
        }
    }

    @PreDestroy
    synchronized void shutdown() {
        tenants.forEach(sharedHttpPorts::detach);
        tenants.clear();
        if (proxyClient != null) {
            proxyClient.close();
        }
    }

    private void attach(int port) {
        if (port == config.port() || tlsProxyServer.reservesPort(port)) {
            if (warnedReservedPorts.add(port)) {
                LOG.warnv("VPC Lattice listener port {0} is reserved by Floci itself; listeners on it serve no "
                        + "traffic. Set floci.tls.aws-https-port=0 (or disable TLS) to free 443.",
                        String.valueOf(port));
            }
            return;
        }
        SharedHttpPorts.Tenant tenant = new SharedHttpPorts.Tenant() {
            @Override
            public boolean servesHost(String host) {
                return routes.containsKey(host);
            }

            @Override
            public void handle(HttpServerRequest request) {
                handleRequest(request, port);
            }
        };
        tenants.put(port, tenant);
        sharedHttpPorts.attach(port, tenant, false)
                .onSuccess(ignored -> LOG.infov("VPC Lattice listener port started on {0}", String.valueOf(port)))
                .onFailure(err -> {
                    tenants.remove(port, tenant);
                    LOG.warnv("VPC Lattice listener port {0} failed to start: {1}", String.valueOf(port),
                            err.getMessage());
                });
    }

    private void refreshCertificates(List<LatticeService> services, List<Listener> listeners) {
        Set<String> httpsServices = new HashSet<>();
        for (Listener listener : listeners) {
            if ("HTTPS".equals(listener.getProtocol())) {
                httpsServices.add(listener.getServiceArn());
            }
        }
        Set<String> wanted = new HashSet<>();
        Set<String> generatedNames = new HashSet<>();
        for (LatticeService svc : services) {
            if (!httpsServices.contains(svc.getArn())) {
                continue;
            }
            if (svc.getDnsEntry() != null && svc.getDnsEntry().getDomainName() != null) {
                String key = CERTIFICATE_KEY_PREFIX + svc.getArn() + ":generated";
                try {
                    sharedHttpPorts.putCertificate(key, generatedCertificate(svc.getId(),
                            svc.getDnsEntry().getDomainName()));
                    wanted.add(key);
                    generatedNames.add(svc.getDnsEntry().getDomainName().toLowerCase(Locale.ROOT));
                } catch (RuntimeException e) {
                    LOG.warnv("Could not issue a certificate for {0}: {1}", svc.getDnsEntry().getDomainName(),
                            e.getMessage());
                }
            }
            if (svc.getCustomDomainName() != null && svc.getCertificateArn() != null) {
                String key = CERTIFICATE_KEY_PREFIX + svc.getArn() + ":custom";
                Optional<SharedHttpPorts.ServerCertificate> certificate = acmCertificate(svc);
                if (certificate.isPresent()) {
                    wanted.add(key);
                    sharedHttpPorts.putCertificate(key, certificate.get());
                }
            }
        }
        for (String key : Set.copyOf(publishedCertificates)) {
            if (!wanted.contains(key)) {
                sharedHttpPorts.removeCertificate(key);
            }
        }
        publishedCertificates.clear();
        publishedCertificates.addAll(wanted);
        generatedCertificates.keySet().retainAll(generatedNames);
    }

    /** The generated name can pass the 64 characters a common name allows, so it goes in the SAN only. */
    private SharedHttpPorts.ServerCertificate generatedCertificate(String serviceId, String domainName) {
        return generatedCertificates.computeIfAbsent(domainName.toLowerCase(Locale.ROOT), name -> {
            CertificateGenerator.GeneratedCertificate issued = certificateAuthority.issueServerCertificate(
                    serviceId, List.of(name), KeyAlgorithm.RSA_2048, null);
            return new SharedHttpPorts.ServerCertificate(issued.certificatePem() + certificateAuthority.caPem(),
                    issued.privateKeyPem());
        });
    }

    private Optional<SharedHttpPorts.ServerCertificate> acmCertificate(LatticeService svc) {
        try {
            Certificate certificate = RequestScopes.callAs(svc.getAccountId(), svc.getRegion(),
                    () -> acmService.getCertificate(svc.getCertificateArn(), svc.getRegion()));
            if (certificate.getPrivateKey() == null || certificate.getCertificateBody() == null) {
                LOG.warnv("ACM certificate {0} of Lattice service {1} has no private key; {2} is served with "
                        + "the default certificate", svc.getCertificateArn(), svc.getId(), svc.getCustomDomainName());
                return Optional.empty();
            }
            String chain = certificate.getCertificateChain() != null ? certificate.getCertificateChain() : "";
            return Optional.of(new SharedHttpPorts.ServerCertificate(
                    certificate.getCertificateBody().strip() + "\n" + chain, certificate.getPrivateKey()));
        } catch (Exception e) {
            LOG.warnv("Could not load ACM certificate {0} for Lattice service {1}: {2}", svc.getCertificateArn(),
                    svc.getId(), e.getMessage());
            return Optional.empty();
        }
    }

    private void handleRequest(HttpServerRequest request, int port) {
        request.pause();
        Route route = routes.get(SharedHttpPorts.normalizeHost(request.host()));
        if (route == null) {
            reject(request, 404, "Not Found");
            return;
        }
        vertx.<Forward>executeBlocking(() -> RequestScopes.callAs(route.accountId(), () -> plan(request, route, port)),
                false)
                .onSuccess(forward -> {
                    if (forward.status() != 0) {
                        reject(request, forward.status(), forward.message());
                    } else {
                        proxy(request, forward.host(), forward.port());
                    }
                })
                .onFailure(err -> {
                    LOG.warnv("VPC Lattice request for {0} failed: {1}", request.host(), err.getMessage());
                    reject(request, 502, "Bad Gateway");
                });
    }

    private record Forward(int status, String message, String host, int port) {

        static Forward reject(int status, String message) {
            return new Forward(status, message, null, 0);
        }
    }

    private Forward plan(HttpServerRequest request, Route route, int port) {
        VpcLatticeService control = service.get();
        LatticeService svc = control.allServices().stream()
                .filter(s -> s.getId().equals(route.serviceId()) && s.getAccountId().equals(route.accountId()))
                .findFirst()
                .orElse(null);
        if (svc == null) {
            return Forward.reject(404, "Not Found");
        }
        Listener listener = control.allListeners().stream()
                .filter(l -> svc.getId().equals(l.getServiceId()) && Integer.valueOf(port).equals(l.getPort()))
                .findFirst()
                .orElse(null);
        if (listener == null) {
            return Forward.reject(404, "Not Found");
        }
        if (requiresSignature(control, svc) && !isSigned(request)) {
            return Forward.reject(403, "AccessDeniedException: User: anonymous is not authorized to perform: "
                    + "vpc-lattice-svcs:Invoke on resource: " + svc.getArn() + "/" + request.method().name()
                    + request.path() + " because no network-based policy allows the vpc-lattice-svcs:Invoke action");
        }
        RuleAction action = listener.getDefaultAction();
        if (action == null) {
            return Forward.reject(404, "Not Found");
        }
        if (action.getFixedResponse() != null) {
            Integer status = action.getFixedResponse().getStatusCode();
            return Forward.reject(status != null ? status : 404, "");
        }
        TargetGroup tg = pickTargetGroup(control, svc, action);
        if (tg == null) {
            return Forward.reject(500, "Internal Server Error");
        }
        List<Target> routable = new ArrayList<>();
        for (Target target : tg.getTargets()) {
            if (healthChecker.isRoutable(tg, target)) {
                routable.add(target);
            }
        }
        List<Target> candidates = routable.isEmpty() ? tg.getTargets() : routable;
        if (candidates.isEmpty()) {
            return Forward.reject(503, "Service Unavailable");
        }
        int index = Math.floorMod(roundRobin.computeIfAbsent(tg.getArn(), k -> new AtomicInteger()).getAndIncrement(),
                candidates.size());
        Target target = candidates.get(index);
        int targetPort = target.getPort() != null ? target.getPort() : tg.getConfig().getPort();
        return new Forward(0, null, target.getId(), targetPort);
    }

    private static TargetGroup pickTargetGroup(VpcLatticeService control, LatticeService svc, RuleAction action) {
        if (action.getForward() == null || action.getForward().getTargetGroups() == null) {
            return null;
        }
        List<WeightedTargetGroup> weighted = action.getForward().getTargetGroups();
        int total = weighted.stream().mapToInt(w -> Math.max(0, w.getWeight() != null ? w.getWeight() : 1)).sum();
        WeightedTargetGroup chosen = weighted.get(weighted.size() - 1);
        if (total > 0) {
            int roll = ThreadLocalRandom.current().nextInt(total);
            for (WeightedTargetGroup candidate : weighted) {
                roll -= Math.max(0, candidate.getWeight() != null ? candidate.getWeight() : 1);
                if (roll < 0) {
                    chosen = candidate;
                    break;
                }
            }
        }
        return control.targetGroupForAccount(svc.getAccountId(), chosen.getTargetGroupIdentifier()).orElse(null);
    }

    /** AWS_IAM on the service or any network it joins refuses unsigned requests; the policy is not evaluated. */
    private static boolean requiresSignature(VpcLatticeService control, LatticeService svc) {
        if ("AWS_IAM".equals(svc.getAuthType())) {
            return true;
        }
        Set<String> networkArns = new HashSet<>();
        for (ServiceNetworkServiceAssociation association : control.allServiceAssociations()) {
            if (svc.getArn().equals(association.getServiceArn())) {
                networkArns.add(association.getServiceNetworkArn());
            }
        }
        return control.allServiceNetworks().stream()
                .filter(sn -> networkArns.contains(sn.getArn()))
                .map(ServiceNetwork::getAuthType)
                .anyMatch("AWS_IAM"::equals);
    }

    private static boolean isSigned(HttpServerRequest request) {
        String authorization = request.getHeader("Authorization");
        if (authorization != null && authorization.startsWith("AWS4-")) {
            return true;
        }
        return request.getParam("X-Amz-Signature") != null;
    }

    private void proxy(HttpServerRequest request, String host, int port) {
        vertx.<String>executeBlocking(() -> SsrfProtection.rejectMetadataAddresses(
                        InetAddress.getAllByName(host), host)[0].getHostAddress(), false)
                .compose(address -> proxyClient().request(new RequestOptions()
                        .setHost(address)
                        .setPort(port)
                        .setURI(request.uri())
                        .setMethod(request.method())))
                .compose(outbound -> {
                    copyRequestHeaders(request, outbound);
                    if (request.getHeader("Content-Length") == null && request.getHeader("Transfer-Encoding") == null) {
                        request.resume();
                        return outbound.send();
                    }
                    return outbound.send(request);
                })
                .onSuccess(response -> {
                    request.response().setStatusCode(response.statusCode());
                    response.headers().forEach(entry -> {
                        if (!HOP_BY_HOP_HEADERS.contains(entry.getKey().toLowerCase(Locale.ROOT))) {
                            request.response().headers().add(entry.getKey(), entry.getValue());
                        }
                    });
                    if (response.getHeader("Content-Length") == null) {
                        request.response().setChunked(true);
                    }
                    response.pipeTo(request.response());
                })
                .onFailure(err -> {
                    LOG.debugv("VPC Lattice proxy to {0}:{1} failed: {2}", host, String.valueOf(port),
                            err.getMessage());
                    if (!request.response().headWritten()) {
                        reject(request, 502, "Bad Gateway");
                    } else {
                        request.response().close();
                    }
                });
    }

    private static void copyRequestHeaders(HttpServerRequest request, HttpClientRequest outbound) {
        request.headers().forEach(entry -> {
            if (!HOP_BY_HOP_HEADERS.contains(entry.getKey().toLowerCase(Locale.ROOT))) {
                outbound.headers().add(entry.getKey(), entry.getValue());
            }
        });
        if (request.getHeader("Content-Length") == null && request.getHeader("Transfer-Encoding") != null) {
            outbound.setChunked(true);
        }
    }

    private static void reject(HttpServerRequest request, int status, String message) {
        request.resume();
        request.response().setStatusCode(status).end(message);
    }

    private synchronized HttpClient proxyClient() {
        if (proxyClient == null) {
            proxyClient = vertx.createHttpClient(new HttpClientOptions()
                    .setMaxPoolSize(100)
                    .setConnectTimeout(5000)
                    .setKeepAlive(true));
        }
        return proxyClient;
    }
}
