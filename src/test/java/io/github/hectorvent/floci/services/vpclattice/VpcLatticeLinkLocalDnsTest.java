package io.github.hectorvent.floci.services.vpclattice;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.services.vpclattice.model.LatticeService;
import io.github.hectorvent.floci.testing.InMemoryStorageFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class VpcLatticeLinkLocalDnsTest {

    private static final String REGION = "ap-southeast-2";

    private VpcLatticeService service;
    private VpcLatticeLinkLocalDns dns;

    @BeforeEach
    void setUp() {
        EmulatorConfig config = mock(EmulatorConfig.class);
        EmulatorConfig.ServicesConfig services = mock(EmulatorConfig.ServicesConfig.class);
        EmulatorConfig.VpcLatticeServiceConfig lattice = mock(EmulatorConfig.VpcLatticeServiceConfig.class);
        when(config.hostname()).thenReturn(Optional.of("floci"));
        when(config.services()).thenReturn(services);
        when(services.vpclattice()).thenReturn(lattice);
        when(lattice.enabled()).thenReturn(true);
        service = new VpcLatticeService(new InMemoryStorageFactory("000000000000"),
                new RegionResolver(REGION, "000000000000"), config, mock(VpcLatticeDataPlane.class));
        dns = new VpcLatticeLinkLocalDns(service, config);
    }

    @Test
    void answersOnlyRegisteredTaskNamespaces() {
        LatticeService svc = service.createService("ecs-orders", "AWS_IAM", "orders.prod.example.internal", null, null,
                Map.of(), REGION);
        dns.register("172.18.0.9");

        assertEquals(List.of(VpcLatticeLinkLocalDns.ADDRESS),
                dns.resolve("orders.prod.example.internal", 1, "172.18.0.9").orElseThrow().addresses());
        assertEquals(List.of(VpcLatticeLinkLocalDns.ADDRESS),
                dns.resolve(svc.getDnsEntry().getDomainName(), 1, "172.18.0.9").orElseThrow().addresses());
        assertTrue(dns.resolve("orders.prod.example.internal", 1, "172.18.0.10").isEmpty());
        assertTrue(dns.resolve("other.prod.example.internal", 1, "172.18.0.9").isEmpty());
    }

    @Test
    void unregisteredNamespaceGetsNoAnswer() {
        service.createService("ecs-orders", "AWS_IAM", "orders.prod.example.internal", null, null, Map.of(), REGION);
        dns.register("172.18.0.9");
        dns.unregister("172.18.0.9");

        assertTrue(dns.resolve("orders.prod.example.internal", 1, "172.18.0.9").isEmpty());
    }
}
