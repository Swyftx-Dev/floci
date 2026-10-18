package com.floci.test;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.vpclattice.VpcLatticeClient;
import software.amazon.awssdk.services.vpclattice.model.AuthType;
import software.amazon.awssdk.services.vpclattice.model.CreateListenerResponse;
import software.amazon.awssdk.services.vpclattice.model.CreateServiceResponse;
import software.amazon.awssdk.services.vpclattice.model.CreateTargetGroupResponse;
import software.amazon.awssdk.services.vpclattice.model.ForwardAction;
import software.amazon.awssdk.services.vpclattice.model.GetServiceResponse;
import software.amazon.awssdk.services.vpclattice.model.GetTargetGroupResponse;
import software.amazon.awssdk.services.vpclattice.model.HealthCheckConfig;
import software.amazon.awssdk.services.vpclattice.model.ListenerProtocol;
import software.amazon.awssdk.services.vpclattice.model.Matcher;
import software.amazon.awssdk.services.vpclattice.model.ResourceNotFoundException;
import software.amazon.awssdk.services.vpclattice.model.RuleAction;
import software.amazon.awssdk.services.vpclattice.model.Target;
import software.amazon.awssdk.services.vpclattice.model.TargetGroupConfig;
import software.amazon.awssdk.services.vpclattice.model.TargetGroupProtocol;
import software.amazon.awssdk.services.vpclattice.model.TargetGroupType;
import software.amazon.awssdk.services.vpclattice.model.WeightedTargetGroup;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class VpcLatticeTest {

    @Test
    void serviceTargetGroupAndListenerLifecycle() {
        try (VpcLatticeClient client = TestFixtures.vpcLatticeClient()) {
            String suffix = Long.toString(System.nanoTime(), 36);
            CreateServiceResponse service = client.createService(b -> b.name("sdk-svc-" + suffix)
                    .authType(AuthType.AWS_IAM).tags(Map.of("team", "sdk")));
            assertTrue(service.dnsEntry().domainName().contains(".vpc-lattice-svcs."));

            CreateTargetGroupResponse targetGroup = client.createTargetGroup(b -> b.name("sdk-tg-" + suffix)
                    .type(TargetGroupType.IP)
                    .config(TargetGroupConfig.builder().port(8080).protocol(TargetGroupProtocol.HTTP)
                            .vpcIdentifier("vpc-sdk")
                            .healthCheck(HealthCheckConfig.builder().path("/health")
                                    .matcher(Matcher.fromHttpCode("200")).build())
                            .build()));

            CreateListenerResponse listener = client.createListener(b -> b.serviceIdentifier(service.id())
                    .name("http").protocol(ListenerProtocol.HTTP).port(80)
                    .defaultAction(RuleAction.fromForward(ForwardAction.builder().targetGroups(
                            WeightedTargetGroup.builder().targetGroupIdentifier(targetGroup.id()).weight(100).build())
                            .build())));
            assertEquals(service.id(), listener.serviceId());

            client.registerTargets(b -> b.targetGroupIdentifier(targetGroup.id())
                    .targets(Target.builder().id("10.0.0.10").port(8080).build()));
            assertEquals(1, client.listTargets(b -> b.targetGroupIdentifier(targetGroup.id())).items().size());

            GetTargetGroupResponse fetched = client.getTargetGroup(b -> b.targetGroupIdentifier(targetGroup.id()));
            assertEquals("/health", fetched.config().healthCheck().path());
            assertEquals("200", fetched.config().healthCheck().matcher().httpCode());
            assertEquals(1, fetched.serviceArns().size());

            GetServiceResponse fetchedService = client.getService(b -> b.serviceIdentifier(service.arn()));
            assertEquals(AuthType.AWS_IAM, fetchedService.authType());
            assertEquals("sdk", client.listTagsForResource(b -> b.resourceArn(service.arn())).tags().get("team"));

            client.deleteListener(b -> b.serviceIdentifier(service.id()).listenerIdentifier(listener.id()));
            client.deleteService(b -> b.serviceIdentifier(service.id()));
            client.deleteTargetGroup(b -> b.targetGroupIdentifier(targetGroup.id()));
            assertThrows(ResourceNotFoundException.class,
                    () -> client.getService(b -> b.serviceIdentifier(service.id())));
        }
    }
}
