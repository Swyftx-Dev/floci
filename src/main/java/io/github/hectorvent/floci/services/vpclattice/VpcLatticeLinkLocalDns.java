package io.github.hectorvent.floci.services.vpclattice;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.dns.DnsAnswer;
import io.github.hectorvent.floci.core.common.dns.DnsClientRecordSource;
import io.github.hectorvent.floci.services.vpclattice.model.LatticeService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Answers a Lattice service's generated name and custom domain with {@link #ADDRESS} to the task
 * namespaces that can reach it, as Lattice answers with a link-local address inside a VPC.
 */
@ApplicationScoped
public class VpcLatticeLinkLocalDns implements DnsClientRecordSource {

    /** Inside the 169.254.171.0/24 range Lattice uses; a proxy on the task network relays it to Floci. */
    public static final String ADDRESS = "169.254.171.1";

    private final VpcLatticeService service;
    private final EmulatorConfig config;
    private final Set<String> clients = ConcurrentHashMap.newKeySet();

    @Inject
    public VpcLatticeLinkLocalDns(VpcLatticeService service, EmulatorConfig config) {
        this.service = service;
        this.config = config;
    }

    /** Whether the data plane runs, and so whether the link-local address is worth holding. */
    public boolean active() {
        return config.services().vpclattice().enabled() && !config.services().vpclattice().mock();
    }

    public void register(String clientAddress) {
        if (clientAddress != null && !clientAddress.isBlank()) {
            clients.add(clientAddress);
        }
    }

    public void unregister(String clientAddress) {
        if (clientAddress != null) {
            clients.remove(clientAddress);
        }
    }

    @Override
    public Optional<DnsAnswer> resolve(String name, int type, String clientAddress) {
        if (!active() || !clients.contains(clientAddress) || !isServiceName(name.toLowerCase(Locale.ROOT))) {
            return Optional.empty();
        }
        return Optional.of(type == 1 ? DnsAnswer.records(List.of(ADDRESS), DnsAnswer.DEFAULT_TTL_SECONDS)
                : DnsAnswer.noData());
    }

    private boolean isServiceName(String name) {
        for (LatticeService svc : service.allServices()) {
            if (svc.getDnsEntry() != null && name.equalsIgnoreCase(svc.getDnsEntry().getDomainName())) {
                return true;
            }
            if (name.equalsIgnoreCase(svc.getCustomDomainName())) {
                return true;
            }
        }
        return false;
    }
}
