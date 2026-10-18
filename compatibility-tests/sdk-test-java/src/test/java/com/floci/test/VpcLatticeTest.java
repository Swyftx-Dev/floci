package com.floci.test;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.vpclattice.VpcLatticeClient;
import software.amazon.awssdk.services.vpclattice.model.AuthType;
import software.amazon.awssdk.services.vpclattice.model.CreateListenerResponse;
import software.amazon.awssdk.services.vpclattice.model.CreateServiceNetworkResponse;
import software.amazon.awssdk.services.vpclattice.model.CreateServiceNetworkServiceAssociationResponse;
import software.amazon.awssdk.services.vpclattice.model.CreateServiceNetworkVpcAssociationResponse;
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
import software.amazon.awssdk.services.vpclattice.model.ServiceNetworkServiceAssociationStatus;
import software.amazon.awssdk.services.vpclattice.model.ServiceNetworkVpcAssociationStatus;
import software.amazon.awssdk.services.vpclattice.model.Target;
import software.amazon.awssdk.services.vpclattice.model.TargetGroupConfig;
import software.amazon.awssdk.services.vpclattice.model.TargetGroupProtocol;
import software.amazon.awssdk.services.vpclattice.model.TargetGroupType;
import software.amazon.awssdk.services.vpclattice.model.WeightedTargetGroup;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("VPC Lattice")
class VpcLatticeTest {

    @Test
    @DisplayName("service, target group and listener lifecycle")
    void serviceTargetGroupAndListenerLifecycle() {
        try (VpcLatticeClient client = TestFixtures.vpcLatticeClient()) {
            String suffix = Long.toString(System.nanoTime(), 36);
            CreateServiceResponse service = client.createService(b -> b.name("sdk-svc-" + suffix)
                    .authType(AuthType.AWS_IAM).tags(Map.of("team", "sdk")));
            assertThat(service.dnsEntry().domainName()).contains(".vpc-lattice-svcs.");

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
            assertThat(listener.serviceId()).isEqualTo(service.id());

            client.registerTargets(b -> b.targetGroupIdentifier(targetGroup.id())
                    .targets(Target.builder().id("10.0.0.10").port(8080).build()));
            assertThat(client.listTargets(b -> b.targetGroupIdentifier(targetGroup.id())).items()).hasSize(1);

            GetTargetGroupResponse fetched = client.getTargetGroup(b -> b.targetGroupIdentifier(targetGroup.id()));
            assertThat(fetched.config().healthCheck().path()).isEqualTo("/health");
            assertThat(fetched.config().healthCheck().matcher().httpCode()).isEqualTo("200");
            assertThat(fetched.serviceArns()).hasSize(1);

            GetServiceResponse fetchedService = client.getService(b -> b.serviceIdentifier(service.arn()));
            assertThat(fetchedService.authType()).isEqualTo(AuthType.AWS_IAM);
            assertThat(client.listTagsForResource(b -> b.resourceArn(service.arn())).tags())
                    .containsEntry("team", "sdk");

            client.deleteListener(b -> b.serviceIdentifier(service.id()).listenerIdentifier(listener.id()));
            client.deleteService(b -> b.serviceIdentifier(service.id()));
            client.deleteTargetGroup(b -> b.targetGroupIdentifier(targetGroup.id()));
            assertThatThrownBy(() -> client.getService(b -> b.serviceIdentifier(service.id())))
                    .isInstanceOf(ResourceNotFoundException.class);
        }
    }

    @Test
    @DisplayName("service network with a service and a VPC associated")
    void serviceNetworkAssociations() {
        try (VpcLatticeClient client = TestFixtures.vpcLatticeClient()) {
            String suffix = Long.toString(System.nanoTime(), 36);
            CreateServiceNetworkResponse network = client.createServiceNetwork(b -> b.name("sdk-sn-" + suffix)
                    .authType(AuthType.NONE));
            CreateServiceResponse service = client.createService(b -> b.name("sdk-sn-svc-" + suffix));

            CreateServiceNetworkServiceAssociationResponse serviceAssociation =
                    client.createServiceNetworkServiceAssociation(b -> b.serviceNetworkIdentifier(network.id())
                            .serviceIdentifier(service.arn()));
            assertThat(serviceAssociation.status()).isIn(ServiceNetworkServiceAssociationStatus.ACTIVE,
                    ServiceNetworkServiceAssociationStatus.CREATE_IN_PROGRESS);
            assertThat(serviceAssociation.dnsEntry().domainName()).isEqualTo(service.dnsEntry().domainName());

            CreateServiceNetworkVpcAssociationResponse vpcAssociation =
                    client.createServiceNetworkVpcAssociation(b -> b.serviceNetworkIdentifier(network.arn())
                            .vpcIdentifier("vpc-sdk-" + suffix));
            assertThat(vpcAssociation.status()).isIn(ServiceNetworkVpcAssociationStatus.ACTIVE,
                    ServiceNetworkVpcAssociationStatus.CREATE_IN_PROGRESS);

            assertThat(client.listServiceNetworkServiceAssociations(b -> b.serviceNetworkIdentifier(network.id()))
                    .items()).extracting(item -> item.serviceId()).containsExactly(service.id());
            assertThat(client.listServiceNetworkVpcAssociations(b -> b.serviceNetworkIdentifier(network.id()))
                    .items()).extracting(item -> item.vpcId()).containsExactly("vpc-sdk-" + suffix);
            assertThat(client.getServiceNetwork(b -> b.serviceNetworkIdentifier(network.id()))
                    .numberOfAssociatedServices()).isEqualTo(1L);

            client.deleteServiceNetworkServiceAssociation(b ->
                    b.serviceNetworkServiceAssociationIdentifier(serviceAssociation.id()));
            client.deleteServiceNetworkVpcAssociation(b ->
                    b.serviceNetworkVpcAssociationIdentifier(vpcAssociation.id()));
            client.deleteService(b -> b.serviceIdentifier(service.id()));
            client.deleteServiceNetwork(b -> b.serviceNetworkIdentifier(network.id()));
            assertThatThrownBy(() -> client.getServiceNetwork(b -> b.serviceNetworkIdentifier(network.id())))
                    .isInstanceOf(ResourceNotFoundException.class);
        }
    }
}
