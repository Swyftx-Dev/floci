package io.github.hectorvent.floci.services.msk;

import io.github.hectorvent.floci.core.common.dns.DnsAnswer;
import io.github.hectorvent.floci.core.common.dns.DnsRecordSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.net.InetAddress;
import java.util.List;
import java.util.Optional;

/** Resolves a cluster's TLS and IAM broker names to Floci, where {@link MskBrokerFrontEnd} serves them. */
@ApplicationScoped
public class MskBrokerDnsRecordSource implements DnsRecordSource {

    private static final Logger LOG = Logger.getLogger(MskBrokerDnsRecordSource.class);

    private final MskBrokerFrontEnd frontEnd;

    @Inject
    public MskBrokerDnsRecordSource(MskBrokerFrontEnd frontEnd) {
        this.frontEnd = frontEnd;
    }

    @Override
    public Optional<DnsAnswer> resolveIpv4(String name) {
        if (name == null || !frontEnd.ownsHost(name)) {
            return Optional.empty();
        }
        try {
            return Optional.of(DnsAnswer.records(List.of(InetAddress.getLocalHost().getHostAddress()),
                    DnsAnswer.DEFAULT_TTL_SECONDS));
        } catch (Exception e) {
            LOG.debugv("Could not determine Floci's address for {0}: {1}", name, e.getMessage());
            return Optional.empty();
        }
    }
}
