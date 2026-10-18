package io.github.hectorvent.floci.services.vpclattice;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsArnUtils;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.common.Resettable;
import io.github.hectorvent.floci.core.common.dns.EmbeddedDnsServer;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.services.vpclattice.model.AccessLogSubscription;
import io.github.hectorvent.floci.services.vpclattice.model.AuthPolicy;
import io.github.hectorvent.floci.services.vpclattice.model.DnsEntry;
import io.github.hectorvent.floci.services.vpclattice.model.DnsOptions;
import io.github.hectorvent.floci.services.vpclattice.model.HealthCheckConfig;
import io.github.hectorvent.floci.services.vpclattice.model.LatticeService;
import io.github.hectorvent.floci.services.vpclattice.model.Listener;
import io.github.hectorvent.floci.services.vpclattice.model.Matcher;
import io.github.hectorvent.floci.services.vpclattice.model.RuleAction;
import io.github.hectorvent.floci.services.vpclattice.model.ServiceNetwork;
import io.github.hectorvent.floci.services.vpclattice.model.ServiceNetworkServiceAssociation;
import io.github.hectorvent.floci.services.vpclattice.model.ServiceNetworkVpcAssociation;
import io.github.hectorvent.floci.services.vpclattice.model.SharingConfig;
import io.github.hectorvent.floci.services.vpclattice.model.Target;
import io.github.hectorvent.floci.services.vpclattice.model.TargetGroup;
import io.github.hectorvent.floci.services.vpclattice.model.TargetGroupConfig;
import io.github.hectorvent.floci.services.vpclattice.model.WeightedTargetGroup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * VPC Lattice control plane: service networks, services, listeners, target groups and their targets,
 * associations, auth policies and access log subscriptions, stored per account through
 * {@link StorageFactory}. A change to routing, listeners, targets or certificates refreshes the data
 * plane, which resyncs the health checker.
 */
@ApplicationScoped
public class VpcLatticeService implements Resettable {

    static final String SERVICE = "vpc-lattice";
    private static final Pattern RESOURCE_NAME = Pattern.compile("[a-z0-9]([a-z0-9-]*[a-z0-9])?");
    private static final Set<String> AUTH_TYPES = Set.of("NONE", "AWS_IAM");
    private static final Set<String> TARGET_GROUP_TYPES = Set.of("IP", "LAMBDA", "INSTANCE", "ALB");
    private static final Set<String> LISTENER_PROTOCOLS = Set.of("HTTP", "HTTPS", "TLS_PASSTHROUGH");
    private static final SecureRandom RANDOM = new SecureRandom();

    private final AccountAwareStorageBackend<ServiceNetwork> serviceNetworks;
    private final AccountAwareStorageBackend<LatticeService> services;
    private final AccountAwareStorageBackend<TargetGroup> targetGroups;
    private final AccountAwareStorageBackend<Listener> listeners;
    private final AccountAwareStorageBackend<ServiceNetworkVpcAssociation> vpcAssociations;
    private final AccountAwareStorageBackend<ServiceNetworkServiceAssociation> serviceAssociations;
    private final AccountAwareStorageBackend<AccessLogSubscription> accessLogSubscriptions;
    private final AccountAwareStorageBackend<AuthPolicy> authPolicies;
    private final RegionResolver regionResolver;
    private final EmulatorConfig config;
    private final VpcLatticeDataPlane dataPlane;

    @Inject
    public VpcLatticeService(StorageFactory storageFactory, RegionResolver regionResolver, EmulatorConfig config,
                             VpcLatticeDataPlane dataPlane) {
        this.serviceNetworks = storageFactory.create("vpclattice", "vpclattice-service-networks.json",
                new TypeReference<Map<String, ServiceNetwork>>() {});
        this.services = storageFactory.create("vpclattice", "vpclattice-services.json",
                new TypeReference<Map<String, LatticeService>>() {});
        this.targetGroups = storageFactory.create("vpclattice", "vpclattice-target-groups.json",
                new TypeReference<Map<String, TargetGroup>>() {});
        this.listeners = storageFactory.create("vpclattice", "vpclattice-listeners.json",
                new TypeReference<Map<String, Listener>>() {});
        this.vpcAssociations = storageFactory.create("vpclattice", "vpclattice-vpc-associations.json",
                new TypeReference<Map<String, ServiceNetworkVpcAssociation>>() {});
        this.serviceAssociations = storageFactory.create("vpclattice", "vpclattice-service-associations.json",
                new TypeReference<Map<String, ServiceNetworkServiceAssociation>>() {});
        this.accessLogSubscriptions = storageFactory.create("vpclattice", "vpclattice-access-log-subscriptions.json",
                new TypeReference<Map<String, AccessLogSubscription>>() {});
        this.authPolicies = storageFactory.create("vpclattice", "vpclattice-auth-policies.json",
                new TypeReference<Map<String, AuthPolicy>>() {});
        this.regionResolver = regionResolver;
        this.config = config;
        this.dataPlane = dataPlane;
    }

    public void restorePersistedRuntime() {
        dataPlane.refresh();
    }

    @Override
    public void clear() {
        dataPlane.refresh();
    }

    public ServiceNetwork createServiceNetwork(String name, String authType, SharingConfig sharingConfig,
                                              Map<String, String> tags, String region) {
        validateName(name, "name", 3, 63);
        validateAuthType(authType);
        if (serviceNetworks.scan(k -> true).stream()
                .anyMatch(sn -> region.equals(sn.getRegion()) && name.equals(sn.getName()))) {
            throw conflict("A service network with name " + name + " already exists.", name, "SERVICE_NETWORK");
        }
        ServiceNetwork sn = new ServiceNetwork();
        sn.setId(newId("sn-"));
        sn.setArn(arn(region, "servicenetwork/" + sn.getId()));
        sn.setName(name);
        sn.setAuthType(authType != null ? authType : "NONE");
        sn.setSharingConfig(sharingConfig);
        stamp(sn.getId(), region, sn::setRegion, sn::setAccountId, sn::setCreatedAt, sn::setLastUpdatedAt);
        sn.setTags(tagsOrEmpty(tags));
        serviceNetworks.put(sn.getId(), sn);
        return sn;
    }

    public ServiceNetwork getServiceNetwork(String identifier, String region) {
        return find(serviceNetworks, identifier, region, ServiceNetwork::getRegion)
                .orElseThrow(() -> notFound("Service network", identifier, "SERVICE_NETWORK"));
    }

    public List<ServiceNetwork> listServiceNetworks(String region) {
        return serviceNetworks.scan(k -> true).stream()
                .filter(sn -> region.equals(sn.getRegion()))
                .sorted(Comparator.comparing(ServiceNetwork::getId))
                .toList();
    }

    public ServiceNetwork updateServiceNetwork(String identifier, String authType, String region) {
        ServiceNetwork sn = ownedServiceNetwork(identifier, region);
        validateAuthType(authType);
        if (authType == null) {
            throw validation("authType is required.");
        }
        sn.setAuthType(authType);
        sn.setLastUpdatedAt(Instant.now());
        serviceNetworks.put(sn.getId(), sn);
        return sn;
    }

    public void deleteServiceNetwork(String identifier, String region) {
        ServiceNetwork sn = ownedServiceNetwork(identifier, region);
        if (!vpcAssociationsOf(sn).isEmpty() || !serviceAssociationsOf(sn.getArn()).isEmpty()) {
            throw conflict("Service network " + sn.getId() + " has associations; delete them first.",
                    sn.getId(), "SERVICE_NETWORK");
        }
        serviceNetworks.delete(sn.getId());
        deleteAttachments(sn.getArn());
        dataPlane.refresh();
    }

    public long associatedVpcCount(ServiceNetwork sn) {
        return vpcAssociationsOf(sn).size();
    }

    public long associatedServiceCount(ServiceNetwork sn) {
        return serviceAssociationsOf(sn.getArn()).size();
    }

    public LatticeService createService(String name, String authType, String customDomainName, String certificateArn,
                                        Integer idleTimeoutSeconds, Map<String, String> tags, String region) {
        validateName(name, "name", 3, 40);
        validateAuthType(authType);
        if (idleTimeoutSeconds != null && (idleTimeoutSeconds < 60 || idleTimeoutSeconds > 600)) {
            throw validation("idleTimeoutSeconds must be between 60 and 600.");
        }
        if (services.scan(k -> true).stream()
                .anyMatch(s -> region.equals(s.getRegion()) && name.equals(s.getName()))) {
            throw conflict("A service with name " + name + " already exists.", name, "SERVICE");
        }
        LatticeService svc = new LatticeService();
        svc.setId(newId("svc-"));
        svc.setArn(arn(region, "service/" + svc.getId()));
        svc.setName(name);
        svc.setAuthType(authType != null ? authType : "NONE");
        svc.setCustomDomainName(blankToNull(customDomainName));
        svc.setCertificateArn(blankToNull(certificateArn));
        svc.setIdleTimeoutSeconds(idleTimeoutSeconds);
        svc.setStatus("ACTIVE");
        stamp(svc.getId(), region, svc::setRegion, svc::setAccountId, svc::setCreatedAt, svc::setLastUpdatedAt);
        svc.setDnsEntry(dnsEntry(svc));
        svc.setTags(tagsOrEmpty(tags));
        services.put(svc.getId(), svc);
        dataPlane.refresh();
        return svc;
    }

    public LatticeService getService(String identifier, String region) {
        return find(services, identifier, region, LatticeService::getRegion)
                .orElseThrow(() -> notFound("Service", identifier, "SERVICE"));
    }

    public List<LatticeService> listServices(String region) {
        return services.scan(k -> true).stream()
                .filter(s -> region.equals(s.getRegion()))
                .sorted(Comparator.comparing(LatticeService::getId))
                .toList();
    }

    public LatticeService updateService(String identifier, String certificateArn, String authType,
                                        Integer idleTimeoutSeconds, String region) {
        LatticeService svc = ownedService(identifier, region);
        validateAuthType(authType);
        if (authType != null) {
            svc.setAuthType(authType);
        }
        if (certificateArn != null) {
            svc.setCertificateArn(blankToNull(certificateArn));
        }
        if (idleTimeoutSeconds != null) {
            svc.setIdleTimeoutSeconds(idleTimeoutSeconds);
        }
        svc.setLastUpdatedAt(Instant.now());
        services.put(svc.getId(), svc);
        dataPlane.refresh();
        return svc;
    }

    public LatticeService deleteService(String identifier, String region) {
        LatticeService svc = ownedService(identifier, region);
        if (!serviceAssociationsOf(svc.getArn()).isEmpty()) {
            throw conflict("Service " + svc.getId() + " is associated with a service network.",
                    svc.getId(), "SERVICE");
        }
        for (Listener listener : listenersOf(svc.getId())) {
            listeners.delete(listener.getId());
            deleteAttachments(listener.getArn());
        }
        services.delete(svc.getId());
        deleteAttachments(svc.getArn());
        dataPlane.refresh();
        svc.setStatus("DELETE_IN_PROGRESS");
        return svc;
    }

    public TargetGroup createTargetGroup(String name, String type, TargetGroupConfig targetGroupConfig,
                                         Map<String, String> tags, String region) {
        validateName(name, "name", 3, 128);
        if (type == null || !TARGET_GROUP_TYPES.contains(type)) {
            throw validation("type must be one of " + TARGET_GROUP_TYPES + ".");
        }
        if (targetGroups.scan(k -> true).stream()
                .anyMatch(tg -> region.equals(tg.getRegion()) && name.equals(tg.getName()))) {
            throw conflict("A target group with name " + name + " already exists.", name, "TARGET_GROUP");
        }
        TargetGroupConfig resolvedConfig = targetGroupConfig;
        if (!"LAMBDA".equals(type)) {
            if (resolvedConfig == null) {
                throw validation("config is required for a target group of type " + type + ".");
            }
            if (resolvedConfig.getProtocol() == null) {
                throw validation("config.protocol is required.");
            }
            if (resolvedConfig.getPort() == null) {
                resolvedConfig.setPort("HTTPS".equals(resolvedConfig.getProtocol()) ? 443 : 80);
            }
            if (resolvedConfig.getProtocolVersion() == null && !"TCP".equals(resolvedConfig.getProtocol())) {
                resolvedConfig.setProtocolVersion("HTTP1");
            }
            if (resolvedConfig.getIpAddressType() == null && "IP".equals(type)) {
                resolvedConfig.setIpAddressType("IPV4");
            }
            if (!"ALB".equals(type)) {
                resolvedConfig.setHealthCheck(withHealthCheckDefaults(resolvedConfig.getHealthCheck(), resolvedConfig));
            }
        }
        TargetGroup tg = new TargetGroup();
        tg.setId(newId("tg-"));
        tg.setArn(arn(region, "targetgroup/" + tg.getId()));
        tg.setName(name);
        tg.setType(type);
        tg.setConfig(resolvedConfig);
        tg.setStatus("ACTIVE");
        stamp(tg.getId(), region, tg::setRegion, tg::setAccountId, tg::setCreatedAt, tg::setLastUpdatedAt);
        tg.setTags(tagsOrEmpty(tags));
        targetGroups.put(tg.getId(), tg);
        dataPlane.refresh();
        return tg;
    }

    public TargetGroup getTargetGroup(String identifier, String region) {
        return find(targetGroups, identifier, region, TargetGroup::getRegion)
                .orElseThrow(() -> notFound("Target group", identifier, "TARGET_GROUP"));
    }

    public List<TargetGroup> listTargetGroups(String vpcIdentifier, String targetGroupType, String region) {
        return targetGroups.scan(k -> true).stream()
                .filter(tg -> region.equals(tg.getRegion()))
                .filter(tg -> vpcIdentifier == null
                        || (tg.getConfig() != null && vpcIdentifier.equals(tg.getConfig().getVpcIdentifier())))
                .filter(tg -> targetGroupType == null || targetGroupType.equals(tg.getType()))
                .sorted(Comparator.comparing(TargetGroup::getId))
                .toList();
    }

    public TargetGroup updateTargetGroup(String identifier, HealthCheckConfig healthCheck, String region) {
        TargetGroup tg = ownedTargetGroup(identifier, region);
        if (healthCheck == null) {
            throw validation("healthCheck is required.");
        }
        if (tg.getConfig() == null) {
            throw validation("Target group " + tg.getId() + " of type " + tg.getType() + " has no health check.");
        }
        tg.getConfig().setHealthCheck(withHealthCheckDefaults(mergeHealthCheck(tg.getConfig().getHealthCheck(),
                healthCheck), tg.getConfig()));
        tg.setLastUpdatedAt(Instant.now());
        targetGroups.put(tg.getId(), tg);
        dataPlane.refresh();
        return tg;
    }

    public TargetGroup deleteTargetGroup(String identifier, String region) {
        TargetGroup tg = ownedTargetGroup(identifier, region);
        if (!serviceArnsUsing(tg).isEmpty()) {
            throw conflict("Target group " + tg.getId() + " is in use by a listener.", tg.getId(), "TARGET_GROUP");
        }
        targetGroups.delete(tg.getId());
        deleteAttachments(tg.getArn());
        dataPlane.refresh();
        tg.setStatus("DELETE_IN_PROGRESS");
        return tg;
    }

    public List<Target> registerTargets(String identifier, List<Target> targets, String region) {
        TargetGroup tg = targetGroupForTargets(identifier, region);
        if (targets == null || targets.isEmpty()) {
            throw validation("targets is required.");
        }
        List<Target> registered = new ArrayList<>();
        for (Target requested : targets) {
            if (requested.getId() == null || requested.getId().isBlank()) {
                throw validation("Each target needs an id.");
            }
            Target target = new Target();
            target.setId(requested.getId());
            target.setPort(requested.getPort() != null ? requested.getPort()
                    : tg.getConfig() != null ? tg.getConfig().getPort() : null);
            if (tg.getTargets().stream().noneMatch(t -> sameTarget(t, target))) {
                tg.getTargets().add(target);
            }
            registered.add(target);
        }
        tg.setLastUpdatedAt(Instant.now());
        targetGroups.putForAccount(tg.getAccountId(), tg.getId(), tg);
        dataPlane.refresh();
        return registered;
    }

    public List<Target> deregisterTargets(String identifier, List<Target> targets, String region) {
        TargetGroup tg = targetGroupForTargets(identifier, region);
        if (targets == null || targets.isEmpty()) {
            throw validation("targets is required.");
        }
        List<Target> deregistered = new ArrayList<>();
        for (Target requested : targets) {
            Target target = new Target();
            target.setId(requested.getId());
            target.setPort(requested.getPort() != null ? requested.getPort()
                    : tg.getConfig() != null ? tg.getConfig().getPort() : null);
            tg.getTargets().removeIf(t -> sameTarget(t, target));
            deregistered.add(target);
        }
        tg.setLastUpdatedAt(Instant.now());
        targetGroups.putForAccount(tg.getAccountId(), tg.getId(), tg);
        dataPlane.refresh();
        return deregistered;
    }

    public record TargetSummary(String id, Integer port, String status, String reasonCode) {}

    public List<TargetSummary> listTargets(String identifier, List<Target> filter, String region) {
        TargetGroup tg = targetGroupForTargets(identifier, region);
        boolean used = !serviceArnsUsing(tg).isEmpty();
        List<TargetSummary> summaries = new ArrayList<>();
        for (Target target : tg.getTargets()) {
            if (filter != null && !filter.isEmpty() && filter.stream().noneMatch(f ->
                    Objects.equals(f.getId(), target.getId())
                            && (f.getPort() == null || f.getPort().equals(target.getPort())))) {
                continue;
            }
            VpcLatticeHealthChecker.Health health = used
                    ? dataPlane.health(tg, target)
                    : new VpcLatticeHealthChecker.Health("UNUSED", "TargetGroupNotInUse");
            summaries.add(new TargetSummary(target.getId(), target.getPort(), health.status(), health.reasonCode()));
        }
        return summaries;
    }

    public List<String> serviceArnsUsing(TargetGroup tg) {
        return listeners.scanForAccount(tg.getAccountId(), k -> true).stream()
                .filter(l -> forwardsTo(l, tg))
                .map(Listener::getServiceArn)
                .distinct()
                .sorted()
                .toList();
    }

    public Listener createListener(String serviceIdentifier, String name, String protocol, Integer port,
                                   RuleAction defaultAction, Map<String, String> tags, String region) {
        LatticeService svc = ownedService(serviceIdentifier, region);
        validateName(name, "name", 3, 63);
        if (protocol == null || !LISTENER_PROTOCOLS.contains(protocol)) {
            throw validation("protocol must be one of " + LISTENER_PROTOCOLS + ".");
        }
        validateDefaultAction(defaultAction, region);
        int resolvedPort = port != null ? port : "HTTP".equals(protocol) ? 80 : 443;
        for (Listener existing : listenersOf(svc.getId())) {
            if (name.equals(existing.getName()) || resolvedPort == existing.getPort()) {
                throw conflict("Service " + svc.getId() + " already has a listener named " + name
                        + " or on port " + resolvedPort + ".", existing.getId(), "LISTENER");
            }
        }
        Listener listener = new Listener();
        listener.setId(newId("listener-"));
        listener.setArn(svc.getArn() + "/listener/" + listener.getId());
        listener.setName(name);
        listener.setProtocol(protocol);
        listener.setPort(resolvedPort);
        listener.setServiceId(svc.getId());
        listener.setServiceArn(svc.getArn());
        listener.setDefaultAction(defaultAction);
        stamp(listener.getId(), region, listener::setRegion, listener::setAccountId, listener::setCreatedAt,
                listener::setLastUpdatedAt);
        listener.setTags(tagsOrEmpty(tags));
        listeners.put(listener.getId(), listener);
        dataPlane.refresh();
        return listener;
    }

    public Listener getListener(String serviceIdentifier, String listenerIdentifier, String region) {
        LatticeService svc = getService(serviceIdentifier, region);
        return listenersOf(svc.getId()).stream()
                .filter(l -> l.getId().equals(listenerIdentifier) || l.getArn().equals(listenerIdentifier))
                .findFirst()
                .orElseThrow(() -> notFound("Listener", listenerIdentifier, "LISTENER"));
    }

    public List<Listener> listListeners(String serviceIdentifier, String region) {
        LatticeService svc = getService(serviceIdentifier, region);
        return listenersOf(svc.getId());
    }

    public Listener updateListener(String serviceIdentifier, String listenerIdentifier, RuleAction defaultAction,
                                   String region) {
        Listener listener = getListener(serviceIdentifier, listenerIdentifier, region);
        validateDefaultAction(defaultAction, region);
        listener.setDefaultAction(defaultAction);
        listener.setLastUpdatedAt(Instant.now());
        listeners.put(listener.getId(), listener);
        dataPlane.refresh();
        return listener;
    }

    public void deleteListener(String serviceIdentifier, String listenerIdentifier, String region) {
        Listener listener = getListener(serviceIdentifier, listenerIdentifier, region);
        listeners.delete(listener.getId());
        deleteAttachments(listener.getArn());
        dataPlane.refresh();
    }

    public ServiceNetworkVpcAssociation createVpcAssociation(String serviceNetworkIdentifier, String vpcIdentifier,
                                                             Boolean privateDnsEnabled, List<String> securityGroupIds,
                                                             DnsOptions dnsOptions, Map<String, String> tags,
                                                             String region) {
        ServiceNetwork sn = getServiceNetwork(require(serviceNetworkIdentifier, "serviceNetworkIdentifier"), region);
        require(vpcIdentifier, "vpcIdentifier");
        if (vpcAssociationsOf(sn).stream().anyMatch(a -> vpcIdentifier.equals(a.getVpcId()))) {
            throw conflict("VPC " + vpcIdentifier + " is already associated with " + sn.getId() + ".",
                    vpcIdentifier, "SERVICE_NETWORK_VPC_ASSOCIATION");
        }
        ServiceNetworkVpcAssociation association = new ServiceNetworkVpcAssociation();
        association.setId(newId("snva-"));
        association.setArn(arn(region, "servicenetworkvpcassociation/" + association.getId()));
        association.setStatus("ACTIVE");
        association.setCreatedBy(regionResolver.getAccountId());
        association.setServiceNetworkId(sn.getId());
        association.setServiceNetworkName(sn.getName());
        association.setServiceNetworkArn(sn.getArn());
        association.setVpcId(vpcIdentifier);
        association.setSecurityGroupIds(securityGroupIds);
        association.setPrivateDnsEnabled(privateDnsEnabled != null ? privateDnsEnabled : Boolean.TRUE);
        association.setDnsOptions(dnsOptions);
        stamp(association.getId(), region, association::setRegion, association::setAccountId,
                association::setCreatedAt, association::setLastUpdatedAt);
        association.setTags(tagsOrEmpty(tags));
        vpcAssociations.put(association.getId(), association);
        dataPlane.refresh();
        return association;
    }

    public ServiceNetworkVpcAssociation getVpcAssociation(String identifier, String region) {
        return find(vpcAssociations, identifier, region, ServiceNetworkVpcAssociation::getRegion)
                .orElseThrow(() -> notFound("Service network VPC association", identifier,
                        "SERVICE_NETWORK_VPC_ASSOCIATION"));
    }

    public List<ServiceNetworkVpcAssociation> listVpcAssociations(String serviceNetworkIdentifier, String vpcIdentifier,
                                                                  String region) {
        if (serviceNetworkIdentifier == null && vpcIdentifier == null) {
            throw validation("Either serviceNetworkIdentifier or vpcIdentifier is required.");
        }
        String snId = serviceNetworkIdentifier == null ? null : idOf(serviceNetworkIdentifier);
        return vpcAssociations.scan(k -> true).stream()
                .filter(a -> region.equals(a.getRegion()))
                .filter(a -> snId == null || snId.equals(a.getServiceNetworkId()))
                .filter(a -> vpcIdentifier == null || vpcIdentifier.equals(a.getVpcId()))
                .sorted(Comparator.comparing(ServiceNetworkVpcAssociation::getId))
                .toList();
    }

    public ServiceNetworkVpcAssociation updateVpcAssociation(String identifier, List<String> securityGroupIds,
                                                             String region) {
        ServiceNetworkVpcAssociation association = ownedVpcAssociation(identifier, region);
        if (securityGroupIds == null || securityGroupIds.isEmpty()) {
            throw validation("securityGroupIds is required.");
        }
        association.setSecurityGroupIds(securityGroupIds);
        association.setLastUpdatedAt(Instant.now());
        vpcAssociations.put(association.getId(), association);
        return association;
    }

    public ServiceNetworkVpcAssociation deleteVpcAssociation(String identifier, String region) {
        ServiceNetworkVpcAssociation association = ownedVpcAssociation(identifier, region);
        vpcAssociations.delete(association.getId());
        deleteAttachments(association.getArn());
        dataPlane.refresh();
        association.setStatus("DELETE_IN_PROGRESS");
        return association;
    }

    public ServiceNetworkServiceAssociation createServiceAssociation(String serviceIdentifier,
                                                                     String serviceNetworkIdentifier,
                                                                     Map<String, String> tags, String region) {
        LatticeService svc = getService(require(serviceIdentifier, "serviceIdentifier"), region);
        ServiceNetwork sn = getServiceNetwork(require(serviceNetworkIdentifier, "serviceNetworkIdentifier"), region);
        if (serviceAssociationsOf(sn.getArn()).stream().anyMatch(a -> svc.getArn().equals(a.getServiceArn()))) {
            throw conflict("Service " + svc.getId() + " is already associated with " + sn.getId() + ".",
                    svc.getId(), "SERVICE_NETWORK_SERVICE_ASSOCIATION");
        }
        if (svc.getCustomDomainName() != null && serviceAssociationsOf(sn.getArn()).stream()
                .map(a -> serviceByArn(a.getServiceArn()))
                .flatMap(Optional::stream)
                .anyMatch(other -> svc.getCustomDomainName().equalsIgnoreCase(other.getCustomDomainName()))) {
            throw conflict("Custom domain " + svc.getCustomDomainName() + " is already in use on " + sn.getId() + ".",
                    svc.getId(), "SERVICE_NETWORK_SERVICE_ASSOCIATION");
        }
        ServiceNetworkServiceAssociation association = new ServiceNetworkServiceAssociation();
        association.setId(newId("snsa-"));
        association.setArn(arn(region, "servicenetworkserviceassociation/" + association.getId()));
        association.setStatus("ACTIVE");
        association.setCreatedBy(regionResolver.getAccountId());
        association.setServiceId(svc.getId());
        association.setServiceName(svc.getName());
        association.setServiceArn(svc.getArn());
        association.setServiceNetworkId(sn.getId());
        association.setServiceNetworkName(sn.getName());
        association.setServiceNetworkArn(sn.getArn());
        stamp(association.getId(), region, association::setRegion, association::setAccountId,
                association::setCreatedAt, association::setLastUpdatedAt);
        association.setTags(tagsOrEmpty(tags));
        serviceAssociations.put(association.getId(), association);
        dataPlane.refresh();
        return association;
    }

    public ServiceNetworkServiceAssociation getServiceAssociation(String identifier, String region) {
        return find(serviceAssociations, identifier, region, ServiceNetworkServiceAssociation::getRegion)
                .orElseThrow(() -> notFound("Service network service association", identifier,
                        "SERVICE_NETWORK_SERVICE_ASSOCIATION"));
    }

    public List<ServiceNetworkServiceAssociation> listServiceAssociations(String serviceNetworkIdentifier,
                                                                          String serviceIdentifier, String region) {
        if (serviceNetworkIdentifier == null && serviceIdentifier == null) {
            throw validation("Either serviceNetworkIdentifier or serviceIdentifier is required.");
        }
        String snId = serviceNetworkIdentifier == null ? null : idOf(serviceNetworkIdentifier);
        String svcId = serviceIdentifier == null ? null : idOf(serviceIdentifier);
        return serviceAssociations.scanAllAccounts().stream()
                .filter(a -> region.equals(a.getRegion()))
                .filter(a -> snId == null || snId.equals(a.getServiceNetworkId()))
                .filter(a -> svcId == null || svcId.equals(a.getServiceId()))
                .filter(a -> regionResolver.getAccountId().equals(a.getAccountId())
                        || regionResolver.getAccountId().equals(accountOf(a.getServiceNetworkArn())))
                .sorted(Comparator.comparing(ServiceNetworkServiceAssociation::getId))
                .toList();
    }

    public ServiceNetworkServiceAssociation deleteServiceAssociation(String identifier, String region) {
        ServiceNetworkServiceAssociation association = find(serviceAssociations, identifier, region,
                ServiceNetworkServiceAssociation::getRegion)
                .orElseThrow(() -> notFound("Service network service association", identifier,
                        "SERVICE_NETWORK_SERVICE_ASSOCIATION"));
        serviceAssociations.deleteForAccount(association.getAccountId(), association.getId());
        deleteAttachments(association.getArn());
        dataPlane.refresh();
        association.setStatus("DELETE_IN_PROGRESS");
        return association;
    }

    /** The service may live in another account than the association. */
    public Optional<LatticeService> serviceByArn(String serviceArn) {
        String account = accountOf(serviceArn);
        return account == null ? Optional.empty() : services.getForAccount(account, idOf(serviceArn));
    }

    public AuthPolicy putAuthPolicy(String resourceIdentifier, String policy, String region) {
        String resourceArn = policyResourceArn(resourceIdentifier, region);
        if (policy == null || policy.isBlank()) {
            throw validation("policy is required.");
        }
        AuthPolicy authPolicy = authPolicies.get(resourceArn).orElseGet(AuthPolicy::new);
        Instant now = Instant.now();
        if (authPolicy.getCreatedAt() == null) {
            authPolicy.setCreatedAt(now);
        }
        authPolicy.setResourceArn(resourceArn);
        authPolicy.setPolicy(policy);
        authPolicy.setLastUpdatedAt(now);
        authPolicies.put(resourceArn, authPolicy);
        return authPolicy;
    }

    public AuthPolicy getAuthPolicy(String resourceIdentifier, String region) {
        String resourceArn = policyResourceArn(resourceIdentifier, region);
        return authPolicies.get(resourceArn)
                .orElseThrow(() -> notFound("Auth policy", resourceIdentifier, "AUTH_POLICY"));
    }

    public void deleteAuthPolicy(String resourceIdentifier, String region) {
        authPolicies.delete(policyResourceArn(resourceIdentifier, region));
    }

    /** Active when the resource's auth type is AWS_IAM, which is when AWS evaluates the policy. */
    public String authPolicyState(String resourceIdentifier, String region) {
        String resourceArn = policyResourceArn(resourceIdentifier, region);
        String authType = resourceArn.contains(":servicenetwork/")
                ? getServiceNetwork(resourceArn, region).getAuthType()
                : getService(resourceArn, region).getAuthType();
        return "AWS_IAM".equals(authType) ? "Active" : "Inactive";
    }

    public AccessLogSubscription createAccessLogSubscription(String resourceIdentifier, String destinationArn,
                                                             String serviceNetworkLogType, Map<String, String> tags,
                                                             String region) {
        String resourceArn = policyResourceArn(require(resourceIdentifier, "resourceIdentifier"), region);
        if (destinationArn == null || !AwsArnUtils.isArn(destinationArn)) {
            throw validation("destinationArn must be the ARN of a log group, S3 bucket or Firehose stream.");
        }
        AccessLogSubscription subscription = new AccessLogSubscription();
        subscription.setId(newId("als-"));
        subscription.setArn(arn(region, "accesslogsubscription/" + subscription.getId()));
        subscription.setResourceArn(resourceArn);
        subscription.setResourceId(idOf(resourceArn));
        subscription.setDestinationArn(destinationArn);
        subscription.setServiceNetworkLogType(serviceNetworkLogType);
        stamp(subscription.getId(), region, subscription::setRegion, subscription::setAccountId,
                subscription::setCreatedAt, subscription::setLastUpdatedAt);
        subscription.setTags(tagsOrEmpty(tags));
        accessLogSubscriptions.put(subscription.getId(), subscription);
        return subscription;
    }

    public AccessLogSubscription getAccessLogSubscription(String identifier, String region) {
        return find(accessLogSubscriptions, identifier, region, AccessLogSubscription::getRegion)
                .orElseThrow(() -> notFound("Access log subscription", identifier, "ACCESS_LOG_SUBSCRIPTION"));
    }

    public List<AccessLogSubscription> listAccessLogSubscriptions(String resourceIdentifier, String region) {
        String resourceArn = policyResourceArn(require(resourceIdentifier, "resourceIdentifier"), region);
        return accessLogSubscriptions.scan(k -> true).stream()
                .filter(s -> resourceArn.equals(s.getResourceArn()))
                .sorted(Comparator.comparing(AccessLogSubscription::getId))
                .toList();
    }

    public AccessLogSubscription updateAccessLogSubscription(String identifier, String destinationArn, String region) {
        AccessLogSubscription subscription = getAccessLogSubscription(identifier, region);
        if (destinationArn == null || !AwsArnUtils.isArn(destinationArn)) {
            throw validation("destinationArn must be an ARN.");
        }
        subscription.setDestinationArn(destinationArn);
        subscription.setLastUpdatedAt(Instant.now());
        accessLogSubscriptions.put(subscription.getId(), subscription);
        return subscription;
    }

    public void deleteAccessLogSubscription(String identifier, String region) {
        AccessLogSubscription subscription = getAccessLogSubscription(identifier, region);
        accessLogSubscriptions.delete(subscription.getId());
        deleteAttachments(subscription.getArn());
    }

    public Map<String, String> listTags(String resourceArn) {
        return tagsHolder(resourceArn).tags();
    }

    public void tagResource(String resourceArn, Map<String, String> tags) {
        if (tags == null) {
            throw validation("tags is required.");
        }
        TagsHolder holder = tagsHolder(resourceArn);
        Map<String, String> merged = new LinkedHashMap<>(holder.tags());
        merged.putAll(tags);
        holder.save().accept(merged);
    }

    public void untagResource(String resourceArn, List<String> tagKeys) {
        TagsHolder holder = tagsHolder(resourceArn);
        Map<String, String> remaining = new LinkedHashMap<>(holder.tags());
        if (tagKeys != null) {
            tagKeys.forEach(remaining::remove);
        }
        holder.save().accept(remaining);
    }

    private record TagsHolder(Map<String, String> tags, Consumer<Map<String, String>> save) {}

    private TagsHolder tagsHolder(String resourceArn) {
        if (resourceArn == null || !AwsArnUtils.isArnFor(resourceArn, SERVICE)) {
            throw notFound("Resource", resourceArn, "RESOURCE");
        }
        String id = idOf(resourceArn);
        String resource = AwsArnUtils.parse(resourceArn).resource();
        if (resource.contains("/listener/")) {
            Listener listener = listeners.get(id).orElseThrow(() -> notFound("Listener", resourceArn, "LISTENER"));
            return new TagsHolder(listener.getTags(), tags -> {
                listener.setTags(tags);
                listeners.put(listener.getId(), listener);
            });
        }
        String type = resource.substring(0, resource.indexOf('/'));
        return switch (type) {
            case "servicenetwork" -> holder(serviceNetworks, id, resourceArn, ServiceNetwork::getTags,
                    ServiceNetwork::setTags);
            case "service" -> holder(services, id, resourceArn, LatticeService::getTags, LatticeService::setTags);
            case "targetgroup" -> holder(targetGroups, id, resourceArn, TargetGroup::getTags, TargetGroup::setTags);
            case "servicenetworkvpcassociation" -> holder(vpcAssociations, id, resourceArn,
                    ServiceNetworkVpcAssociation::getTags, ServiceNetworkVpcAssociation::setTags);
            case "servicenetworkserviceassociation" -> holder(serviceAssociations, id, resourceArn,
                    ServiceNetworkServiceAssociation::getTags, ServiceNetworkServiceAssociation::setTags);
            case "accesslogsubscription" -> holder(accessLogSubscriptions, id, resourceArn,
                    AccessLogSubscription::getTags, AccessLogSubscription::setTags);
            default -> throw notFound("Resource", resourceArn, "RESOURCE");
        };
    }

    private <T> TagsHolder holder(AccountAwareStorageBackend<T> store, String id, String resourceArn,
                                  Function<T, Map<String, String>> getter,
                                  BiConsumer<T, Map<String, String>> setter) {
        T resource = store.get(id).orElseThrow(() -> notFound("Resource", resourceArn, "RESOURCE"));
        Map<String, String> tags = getter.apply(resource);
        return new TagsHolder(tags != null ? tags : Map.of(), updated -> {
            setter.accept(resource, updated);
            store.put(id, resource);
        });
    }

    public List<LatticeService> allServices() {
        return services.scanAllAccounts();
    }

    public List<Listener> allListeners() {
        return listeners.scanAllAccounts();
    }

    public List<TargetGroup> allTargetGroups() {
        return targetGroups.scanAllAccounts();
    }

    public List<ServiceNetwork> allServiceNetworks() {
        return serviceNetworks.scanAllAccounts();
    }

    public List<ServiceNetworkVpcAssociation> allVpcAssociations() {
        return vpcAssociations.scanAllAccounts();
    }

    public List<ServiceNetworkServiceAssociation> allServiceAssociations() {
        return serviceAssociations.scanAllAccounts();
    }

    public Optional<TargetGroup> targetGroupForAccount(String accountId, String identifier) {
        return targetGroups.getForAccount(accountId, idOf(identifier));
    }

    private List<Listener> listenersOf(String serviceId) {
        return listeners.scan(k -> true).stream()
                .filter(l -> serviceId.equals(l.getServiceId()))
                .sorted(Comparator.comparing(Listener::getId))
                .toList();
    }

    private List<ServiceNetworkVpcAssociation> vpcAssociationsOf(ServiceNetwork sn) {
        return vpcAssociations.scanAllAccounts().stream()
                .filter(a -> sn.getArn().equals(a.getServiceNetworkArn()))
                .toList();
    }

    private List<ServiceNetworkServiceAssociation> serviceAssociationsOf(String arn) {
        return serviceAssociations.scanAllAccounts().stream()
                .filter(a -> arn.equals(a.getServiceNetworkArn()) || arn.equals(a.getServiceArn()))
                .toList();
    }

    private static boolean forwardsTo(Listener listener, TargetGroup tg) {
        RuleAction action = listener.getDefaultAction();
        if (action == null || action.getForward() == null || action.getForward().getTargetGroups() == null) {
            return false;
        }
        for (WeightedTargetGroup weighted : action.getForward().getTargetGroups()) {
            String identifier = weighted.getTargetGroupIdentifier();
            if (tg.getId().equals(identifier) || tg.getArn().equals(identifier)) {
                return true;
            }
        }
        return false;
    }

    private void validateDefaultAction(RuleAction action, String region) {
        if (action == null) {
            throw validation("defaultAction is required.");
        }
        boolean forward = action.getForward() != null;
        boolean fixed = action.getFixedResponse() != null;
        if (forward == fixed) {
            throw validation("defaultAction must set exactly one of forward or fixedResponse.");
        }
        if (fixed) {
            Integer status = action.getFixedResponse().getStatusCode();
            if (status == null || status < 100 || status > 599) {
                throw validation("fixedResponse.statusCode must be between 100 and 599.");
            }
            return;
        }
        List<WeightedTargetGroup> groups = action.getForward().getTargetGroups();
        if (groups == null || groups.isEmpty()) {
            throw validation("forward.targetGroups is required.");
        }
        for (WeightedTargetGroup group : groups) {
            getTargetGroup(require(group.getTargetGroupIdentifier(), "targetGroupIdentifier"), region);
            if (group.getWeight() == null) {
                group.setWeight(1);
            }
        }
    }

    private static HealthCheckConfig withHealthCheckDefaults(HealthCheckConfig healthCheck,
                                                             TargetGroupConfig tgConfig) {
        HealthCheckConfig result = healthCheck != null ? healthCheck : new HealthCheckConfig();
        if (result.getEnabled() == null) {
            result.setEnabled(Boolean.TRUE);
        }
        if (result.getProtocol() == null) {
            result.setProtocol(tgConfig.getProtocol());
        }
        if (result.getProtocolVersion() == null) {
            result.setProtocolVersion("HTTP1");
        }
        if (result.getPath() == null) {
            result.setPath("/");
        }
        if (result.getHealthCheckIntervalSeconds() == null) {
            result.setHealthCheckIntervalSeconds(30);
        }
        if (result.getHealthCheckTimeoutSeconds() == null) {
            result.setHealthCheckTimeoutSeconds(5);
        }
        if (result.getHealthyThresholdCount() == null) {
            result.setHealthyThresholdCount(5);
        }
        if (result.getUnhealthyThresholdCount() == null) {
            result.setUnhealthyThresholdCount(2);
        }
        if (result.getMatcher() == null || result.getMatcher().getHttpCode() == null) {
            Matcher matcher = new Matcher();
            matcher.setHttpCode("200");
            result.setMatcher(matcher);
        }
        return result;
    }

    private static HealthCheckConfig mergeHealthCheck(HealthCheckConfig current, HealthCheckConfig update) {
        HealthCheckConfig merged = current != null ? current : new HealthCheckConfig();
        if (update.getEnabled() != null) {
            merged.setEnabled(update.getEnabled());
        }
        if (update.getProtocol() != null) {
            merged.setProtocol(update.getProtocol());
        }
        if (update.getProtocolVersion() != null) {
            merged.setProtocolVersion(update.getProtocolVersion());
        }
        if (update.getPort() != null) {
            merged.setPort(update.getPort());
        }
        if (update.getPath() != null) {
            merged.setPath(update.getPath());
        }
        if (update.getHealthCheckIntervalSeconds() != null) {
            merged.setHealthCheckIntervalSeconds(update.getHealthCheckIntervalSeconds());
        }
        if (update.getHealthCheckTimeoutSeconds() != null) {
            merged.setHealthCheckTimeoutSeconds(update.getHealthCheckTimeoutSeconds());
        }
        if (update.getHealthyThresholdCount() != null) {
            merged.setHealthyThresholdCount(update.getHealthyThresholdCount());
        }
        if (update.getUnhealthyThresholdCount() != null) {
            merged.setUnhealthyThresholdCount(update.getUnhealthyThresholdCount());
        }
        if (update.getMatcher() != null) {
            merged.setMatcher(update.getMatcher());
        }
        return merged;
    }

    private DnsEntry dnsEntry(LatticeService svc) {
        String prefix = svc.getName().length() > 20 ? svc.getName().substring(0, 20) : svc.getName();
        String suffix = config.hostname().filter(h -> !h.isBlank()).orElse(EmbeddedDnsServer.DEFAULT_SUFFIX);
        DnsEntry entry = new DnsEntry();
        entry.setDomainName(prefix + "-" + svc.getId().substring("svc-".length()) + "."
                + sha256Hex(svc.getAccountId()).substring(0, 7) + ".vpc-lattice-svcs." + svc.getRegion() + "."
                + suffix.toLowerCase(Locale.ROOT));
        entry.setHostedZoneId("Z" + sha256Hex("vpc-lattice-svcs:" + svc.getRegion()).substring(0, 20)
                .toUpperCase(Locale.ROOT));
        return entry;
    }

    private String policyResourceArn(String identifier, String region) {
        String id = idOf(require(identifier, "resourceIdentifier"));
        if (id.startsWith("sn-")) {
            return getServiceNetwork(identifier, region).getArn();
        }
        if (id.startsWith("svc-")) {
            return getService(identifier, region).getArn();
        }
        throw notFound("Resource", identifier, "RESOURCE");
    }

    private TargetGroup targetGroupForTargets(String identifier, String region) {
        require(identifier, "targetGroupIdentifier");
        String account = AwsArnUtils.isArn(identifier) ? accountOf(identifier) : regionResolver.getAccountId();
        return targetGroups.getForAccount(account, idOf(identifier))
                .filter(tg -> region.equals(tg.getRegion()))
                .orElseThrow(() -> notFound("Target group", identifier, "TARGET_GROUP"));
    }

    private ServiceNetwork ownedServiceNetwork(String identifier, String region) {
        return owned(serviceNetworks, identifier, region, ServiceNetwork::getRegion)
                .orElseThrow(() -> notFound("Service network", identifier, "SERVICE_NETWORK"));
    }

    private LatticeService ownedService(String identifier, String region) {
        return owned(services, identifier, region, LatticeService::getRegion)
                .orElseThrow(() -> notFound("Service", identifier, "SERVICE"));
    }

    private TargetGroup ownedTargetGroup(String identifier, String region) {
        return owned(targetGroups, identifier, region, TargetGroup::getRegion)
                .orElseThrow(() -> notFound("Target group", identifier, "TARGET_GROUP"));
    }

    private ServiceNetworkVpcAssociation ownedVpcAssociation(String identifier, String region) {
        return owned(vpcAssociations, identifier, region, ServiceNetworkVpcAssociation::getRegion)
                .orElseThrow(() -> notFound("Service network VPC association", identifier,
                        "SERVICE_NETWORK_VPC_ASSOCIATION"));
    }

    /** By id in the caller's account, or by ARN in the ARN's account, so a RAM-shared resource resolves. */
    private <T> Optional<T> find(AccountAwareStorageBackend<T> store, String identifier, String region,
                                 Function<T, String> regionOf) {
        if (identifier == null || identifier.isBlank()) {
            return Optional.empty();
        }
        String account = AwsArnUtils.isArn(identifier) ? accountOf(identifier) : regionResolver.getAccountId();
        if (account == null) {
            return Optional.empty();
        }
        return store.getForAccount(account, idOf(identifier)).filter(r -> region.equals(regionOf.apply(r)));
    }

    private <T> Optional<T> owned(AccountAwareStorageBackend<T> store, String identifier, String region,
                                  Function<T, String> regionOf) {
        if (identifier == null || identifier.isBlank()) {
            return Optional.empty();
        }
        return store.get(idOf(identifier)).filter(r -> region.equals(regionOf.apply(r)));
    }

    private void deleteAttachments(String resourceArn) {
        authPolicies.delete(resourceArn);
        for (AccessLogSubscription subscription : accessLogSubscriptions.scan(k -> true)) {
            if (resourceArn.equals(subscription.getResourceArn())) {
                accessLogSubscriptions.delete(subscription.getId());
            }
        }
    }

    private void stamp(String id, String region, Consumer<String> setRegion,
                       Consumer<String> setAccount, Consumer<Instant> setCreated,
                       Consumer<Instant> setUpdated) {
        Instant now = Instant.now();
        setRegion.accept(region);
        setAccount.accept(regionResolver.getAccountId());
        setCreated.accept(now);
        setUpdated.accept(now);
    }

    private String arn(String region, String resource) {
        return regionResolver.buildArn(SERVICE, region, resource);
    }

    static String idOf(String identifier) {
        if (identifier == null) {
            return null;
        }
        int slash = identifier.lastIndexOf('/');
        return AwsArnUtils.isArn(identifier) && slash >= 0 ? identifier.substring(slash + 1) : identifier;
    }

    private static String accountOf(String arn) {
        if (!AwsArnUtils.isArn(arn)) {
            return null;
        }
        String account = AwsArnUtils.parse(arn).accountId();
        return account.isBlank() ? null : account;
    }

    private static boolean sameTarget(Target a, Target b) {
        return Objects.equals(a.getId(), b.getId()) && Objects.equals(a.getPort(), b.getPort());
    }

    private static String newId(String prefix) {
        byte[] bytes = new byte[9];
        RANDOM.nextBytes(bytes);
        return prefix + HexFormat.of().formatHex(bytes).substring(0, 17);
    }

    private static String sha256Hex(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(String.valueOf(value).getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static Map<String, String> tagsOrEmpty(Map<String, String> tags) {
        return tags != null ? new LinkedHashMap<>(tags) : new LinkedHashMap<>();
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static void validateName(String name, String field, int min, int max) {
        if (name == null || name.length() < min || name.length() > max || !RESOURCE_NAME.matcher(name).matches()) {
            throw validation(field + " must be " + min + " to " + max + " lowercase letters, numbers or hyphens,"
                    + " neither starting nor ending with a hyphen.");
        }
    }

    private static void validateAuthType(String authType) {
        if (authType != null && !AUTH_TYPES.contains(authType)) {
            throw validation("authType must be one of " + AUTH_TYPES + ".");
        }
    }

    private static String require(String value, String field) {
        if (value == null || value.isBlank()) {
            throw validation(field + " is required.");
        }
        return value;
    }

    static AwsException validation(String message) {
        return new AwsException("ValidationException", message, 400);
    }

    private static AwsException notFound(String what, String identifier, String resourceType) {
        return new AwsException("ResourceNotFoundException", what + " " + identifier + " not found.", 404,
                Map.of("resourceId", String.valueOf(identifier), "resourceType", resourceType));
    }

    private static AwsException conflict(String message, String resourceId, String resourceType) {
        return new AwsException("ConflictException", message, 409,
                Map.of("resourceId", resourceId, "resourceType", resourceType));
    }
}
