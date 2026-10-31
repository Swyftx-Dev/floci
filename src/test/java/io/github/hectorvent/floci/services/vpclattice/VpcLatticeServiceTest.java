package io.github.hectorvent.floci.services.vpclattice;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.services.vpclattice.model.DnsOptions;
import io.github.hectorvent.floci.services.vpclattice.model.ForwardAction;
import io.github.hectorvent.floci.services.vpclattice.model.HealthCheckConfig;
import io.github.hectorvent.floci.services.vpclattice.model.LatticeService;
import io.github.hectorvent.floci.services.vpclattice.model.RuleAction;
import io.github.hectorvent.floci.services.vpclattice.model.Target;
import io.github.hectorvent.floci.services.vpclattice.model.TargetGroup;
import io.github.hectorvent.floci.services.vpclattice.model.TargetGroupConfig;
import io.github.hectorvent.floci.services.vpclattice.model.WeightedTargetGroup;
import io.github.hectorvent.floci.testing.InMemoryStorageFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class VpcLatticeServiceTest {

    private static final String REGION = "ap-southeast-2";

    private VpcLatticeService service;
    private VpcLatticeDataPlane dataPlane;

    @BeforeEach
    void setUp() {
        EmulatorConfig config = mock(EmulatorConfig.class);
        when(config.hostname()).thenReturn(Optional.of("floci"));
        dataPlane = mock(VpcLatticeDataPlane.class);
        service = new VpcLatticeService(new InMemoryStorageFactory("000000000000"),
                new RegionResolver(REGION, "000000000000"), config, dataPlane);
    }

    @Test
    void generatedDnsNameSitsUnderFlociHostname() {
        LatticeService svc = service.createService("ecs-inventory-service-long", "AWS_IAM", null, null, null,
                Map.of(), REGION);

        String name = svc.getDnsEntry().getDomainName();
        assertTrue(name.startsWith("ecs-inventory-servic-" + svc.getId().substring(4) + "."), name);
        assertTrue(name.endsWith(".vpc-lattice-svcs.ap-southeast-2.floci"), name);
        assertTrue(svc.getDnsEntry().getHostedZoneId().matches("Z[0-9A-F]{20}"));
        verify(dataPlane, atLeastOnce()).refresh();
    }

    @Test
    void healthCheckDefaultsFillWhatTheCallerOmitted() {
        TargetGroupConfig config = new TargetGroupConfig();
        config.setPort(3800);
        config.setProtocol("HTTP");
        HealthCheckConfig healthCheck = new HealthCheckConfig();
        healthCheck.setPath("/healthcheck");
        config.setHealthCheck(healthCheck);

        TargetGroup tg = service.createTargetGroup("ecs-echo-3800", "IP", config, Map.of(), REGION);

        HealthCheckConfig stored = tg.getConfig().getHealthCheck();
        assertEquals("/healthcheck", stored.getPath());
        assertEquals(30, stored.getHealthCheckIntervalSeconds());
        assertEquals(5, stored.getHealthyThresholdCount());
        assertEquals("200", stored.getMatcher().getHttpCode());
        assertEquals("HTTP1", tg.getConfig().getProtocolVersion());
        assertEquals("IPV4", tg.getConfig().getIpAddressType());
    }

    @Test
    void registeringATargetTwiceKeepsOneAndDefaultsItsPort() {
        TargetGroupConfig config = new TargetGroupConfig();
        config.setPort(3800);
        config.setProtocol("HTTP");
        TargetGroup tg = service.createTargetGroup("ecs-dup-3800", "IP", config, Map.of(), REGION);
        Target target = new Target();
        target.setId("10.0.0.9");

        service.registerTargets(tg.getId(), List.of(target), REGION);
        service.registerTargets(tg.getArn(), List.of(target), REGION);

        List<VpcLatticeService.TargetSummary> targets = service.listTargets(tg.getId(), null, REGION);
        assertEquals(1, targets.size());
        assertEquals(3800, targets.getFirst().port());
        assertEquals("UNUSED", targets.getFirst().status(), "no listener forwards to the group yet");
    }

    @Test
    void aListenerMustForwardToAnExistingTargetGroup() {
        LatticeService svc = service.createService("ecs-orphan", "NONE", null, null, null, Map.of(), REGION);
        AwsException error = assertThrows(AwsException.class, () -> service.createListener(svc.getId(), "https",
                "HTTPS", null, forwardTo("tg-00000000000000000"), Map.of(), REGION));
        assertEquals("ResourceNotFoundException", error.getErrorCode());
    }

    private static RuleAction forwardTo(String targetGroupIdentifier) {
        WeightedTargetGroup weighted = new WeightedTargetGroup();
        weighted.setTargetGroupIdentifier(targetGroupIdentifier);
        weighted.setWeight(100);
        ForwardAction forward = new ForwardAction();
        forward.setTargetGroups(List.of(weighted));
        RuleAction action = new RuleAction();
        action.setForward(forward);
        return action;
    }

    @Test
    void privateDnsHonoursTheSpecifiedDomains() {
        DnsOptions specified = new DnsOptions();
        specified.setPrivateDnsPreference("SPECIFIED_DOMAINS_ONLY");
        specified.setPrivateDnsSpecifiedDomains(List.of("*.prod.example.lattice.internal"));

        assertTrue(VpcLatticeDnsRecordSource.allows(specified, "orders.prod.example.lattice.internal"));
        assertFalse(VpcLatticeDnsRecordSource.allows(specified, "prod.example.lattice.internal"));
        assertFalse(VpcLatticeDnsRecordSource.allows(specified, "orders.dev.example.lattice.internal"));
        assertTrue(VpcLatticeDnsRecordSource.allows(null, "anything.example"));

        DnsOptions verifiedOnly = new DnsOptions();
        verifiedOnly.setPrivateDnsPreference("VERIFIED_DOMAINS_ONLY");
        assertFalse(VpcLatticeDnsRecordSource.allows(verifiedOnly, "orders.prod.example.lattice.internal"));
    }
}
