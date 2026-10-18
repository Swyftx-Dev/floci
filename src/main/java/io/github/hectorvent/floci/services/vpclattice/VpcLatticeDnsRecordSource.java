package io.github.hectorvent.floci.services.vpclattice;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.DomainNames;
import io.github.hectorvent.floci.core.common.dns.DnsAnswer;
import io.github.hectorvent.floci.core.common.dns.DnsRecordSource;
import io.github.hectorvent.floci.services.vpclattice.model.DnsOptions;
import io.github.hectorvent.floci.services.vpclattice.model.LatticeService;
import io.github.hectorvent.floci.services.vpclattice.model.ServiceNetworkServiceAssociation;
import io.github.hectorvent.floci.services.vpclattice.model.ServiceNetworkVpcAssociation;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Answers custom domains only: generated names already sit under Floci's own DNS suffix. */
@ApplicationScoped
public class VpcLatticeDnsRecordSource implements DnsRecordSource {

    private final VpcLatticeService service;
    private final EmulatorConfig config;

    @Inject
    public VpcLatticeDnsRecordSource(VpcLatticeService service, EmulatorConfig config) {
        this.service = service;
        this.config = config;
    }

    @Override
    public Optional<DnsAnswer> resolveIpv4(String name) {
        if (name == null || name.isBlank() || !config.services().vpclattice().enabled()
                || config.services().vpclattice().mock()) {
            return Optional.empty();
        }
        String lower = name.toLowerCase(Locale.ROOT);
        List<LatticeService> holders = service.allServices().stream()
                .filter(s -> lower.equals(s.getCustomDomainName() == null ? null
                        : s.getCustomDomainName().toLowerCase(Locale.ROOT)))
                .toList();
        if (holders.isEmpty()) {
            return Optional.empty();
        }
        List<ServiceNetworkServiceAssociation> associations = service.allServiceAssociations();
        List<ServiceNetworkVpcAssociation> vpcAssociations = service.allVpcAssociations();
        for (LatticeService holder : holders) {
            Set<String> networks = associations.stream()
                    .filter(a -> holder.getArn().equals(a.getServiceArn()))
                    .map(ServiceNetworkServiceAssociation::getServiceNetworkArn)
                    .collect(Collectors.toSet());
            boolean resolvable = vpcAssociations.stream()
                    .filter(a -> networks.contains(a.getServiceNetworkArn()))
                    .anyMatch(a -> Boolean.TRUE.equals(a.getPrivateDnsEnabled()) && allows(a.getDnsOptions(), lower));
            if (resolvable) {
                return VpcLatticeDataPlane.flociAddress()
                        .map(address -> DnsAnswer.records(List.of(address), DnsAnswer.DEFAULT_TTL_SECONDS));
            }
        }
        return Optional.empty();
    }

    /** Domain verification is not emulated, so only ALL_DOMAINS and the specified domains resolve. */
    static boolean allows(DnsOptions options, String name) {
        if (options == null || options.getPrivateDnsPreference() == null
                || "ALL_DOMAINS".equals(options.getPrivateDnsPreference())) {
            return true;
        }
        if ("VERIFIED_DOMAINS_ONLY".equals(options.getPrivateDnsPreference())
                || options.getPrivateDnsSpecifiedDomains() == null) {
            return false;
        }
        for (String domain : options.getPrivateDnsSpecifiedDomains()) {
            String pattern = domain.toLowerCase(Locale.ROOT);
            if (pattern.startsWith("*.")) {
                String parent = pattern.substring(2);
                if (!name.equals(parent) && DomainNames.isWithin(name, parent)) {
                    return true;
                }
            } else if (pattern.equals(name)) {
                return true;
            }
        }
        return false;
    }
}
