package io.github.hectorvent.floci.core.common.dns;

import java.util.Optional;

/**
 * A source whose answer depends on who asks, consulted before every other source: AWS answers some
 * names differently inside a VPC, as VPC Lattice answers its service names with link-local addresses.
 */
public interface DnsClientRecordSource {

    /** Empty leaves the query to the other sources. The name arrives without a trailing dot. */
    Optional<DnsAnswer> resolve(String name, int type, String clientAddress);
}
