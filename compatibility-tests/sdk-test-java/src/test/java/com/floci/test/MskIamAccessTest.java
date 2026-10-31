package com.floci.test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.BrokerNodeGroupInfo;
import software.amazon.awssdk.services.kafka.model.ClientAuthentication;
import software.amazon.awssdk.services.kafka.model.ClientBroker;
import software.amazon.awssdk.services.kafka.model.CreateVpcConnectionResponse;
import software.amazon.awssdk.services.kafka.model.DescribeVpcConnectionResponse;
import software.amazon.awssdk.services.kafka.model.EncryptionInTransit;
import software.amazon.awssdk.services.kafka.model.EncryptionInfo;
import software.amazon.awssdk.services.kafka.model.GetBootstrapBrokersResponse;
import software.amazon.awssdk.services.kafka.model.GetClusterPolicyResponse;
import software.amazon.awssdk.services.kafka.model.Iam;
import software.amazon.awssdk.services.kafka.model.NotFoundException;
import software.amazon.awssdk.services.kafka.model.Sasl;
import software.amazon.awssdk.services.kafka.model.VpcConnection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("MSK IAM access, cluster policy and VPC connections")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MskIamAccessTest {

    private static final String CLUSTER_NAME = TestFixtures.uniqueName("msk-iam");

    private static KafkaClient kafka;
    private static String clusterArn;
    private static String vpcConnectionArn;

    @BeforeAll
    static void setup() {
        kafka = TestFixtures.kafkaClient();
        clusterArn = kafka.createCluster(b -> b.clusterName(CLUSTER_NAME)
                .kafkaVersion("3.6.1")
                .numberOfBrokerNodes(2)
                .brokerNodeGroupInfo(BrokerNodeGroupInfo.builder()
                        .instanceType("kafka.m5.large")
                        .clientSubnets("subnet-1", "subnet-2")
                        .build())
                .clientAuthentication(ClientAuthentication.builder()
                        .sasl(Sasl.builder().iam(Iam.builder().enabled(true).build()).build())
                        .build())
                .encryptionInfo(EncryptionInfo.builder()
                        .encryptionInTransit(EncryptionInTransit.builder()
                                .clientBroker(ClientBroker.TLS).inCluster(true).build())
                        .build()))
                .clusterArn();
    }

    @AfterAll
    static void cleanup() {
        if (kafka != null) {
            if (vpcConnectionArn != null) {
                try {
                    kafka.deleteVpcConnection(b -> b.arn(vpcConnectionArn));
                } catch (Exception ignored) {}
            }
            if (clusterArn != null) {
                try {
                    kafka.deleteCluster(b -> b.clusterArn(clusterArn));
                } catch (Exception ignored) {}
            }
            kafka.close();
        }
    }

    @Test
    @Order(1)
    @DisplayName("GetBootstrapBrokers returns a SASL/IAM string on 9098 for every broker")
    void bootstrapBrokersForIam() {
        GetBootstrapBrokersResponse response = kafka.getBootstrapBrokers(b -> b.clusterArn(clusterArn));

        assertThat(response.bootstrapBrokerStringSaslIam()).isNotNull();
        String[] brokers = response.bootstrapBrokerStringSaslIam().split(",");
        assertThat(brokers).hasSize(2);
        assertThat(brokers[0]).startsWith("b-1.").contains(".kafka.").endsWith(":9098");
        assertThat(brokers[1]).startsWith("b-2.").endsWith(":9098");
        assertThat(response.bootstrapBrokerStringTls()).isNull();
    }

    @Test
    @Order(2)
    @DisplayName("cluster policy round-trips with a version and is removed on delete")
    void clusterPolicyLifecycle() {
        String policy = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\","
                + "\"Principal\":{\"AWS\":\"" + TestFixtures.globalArn("iam", "111111111111", "root") + "\"},"
                + "\"Action\":[\"kafka:CreateVpcConnection\",\"kafka:GetBootstrapBrokers\"],"
                + "\"Resource\":\"" + clusterArn + "\"}]}";

        String version = kafka.putClusterPolicy(b -> b.clusterArn(clusterArn).policy(policy)).currentVersion();
        GetClusterPolicyResponse fetched = kafka.getClusterPolicy(b -> b.clusterArn(clusterArn));
        assertThat(version).isNotBlank();
        assertThat(fetched.currentVersion()).isEqualTo(version);
        assertThat(fetched.policy()).contains("kafka:CreateVpcConnection");

        kafka.deleteClusterPolicy(b -> b.clusterArn(clusterArn));
        assertThatThrownBy(() -> kafka.getClusterPolicy(b -> b.clusterArn(clusterArn)))
                .isInstanceOf(NotFoundException.class);
    }

    @Test
    @Order(3)
    @DisplayName("VPC connection is created, listed for the client and the cluster, and deleted")
    void vpcConnectionLifecycle() {
        CreateVpcConnectionResponse created = kafka.createVpcConnection(b -> b.targetClusterArn(clusterArn)
                .authentication("SASL_IAM")
                .vpcId("vpc-client")
                .clientSubnets("subnet-a", "subnet-b")
                .securityGroups("sg-client"));
        vpcConnectionArn = created.vpcConnectionArn();
        assertThat(vpcConnectionArn).isNotBlank();

        DescribeVpcConnectionResponse described = kafka.describeVpcConnection(b -> b.arn(vpcConnectionArn));
        assertThat(described.targetClusterArn()).isEqualTo(clusterArn);
        assertThat(described.vpcId()).isEqualTo("vpc-client");
        assertThat(described.authentication()).isEqualTo("SASL_IAM");
        assertThat(described.creationTime()).isNotNull();

        assertThat(kafka.listVpcConnections(b -> {}).vpcConnections())
                .extracting(VpcConnection::vpcConnectionArn).contains(vpcConnectionArn);
        assertThat(kafka.listClientVpcConnections(b -> b.clusterArn(clusterArn)).clientVpcConnections())
                .anyMatch(c -> c.vpcConnectionArn().equals(vpcConnectionArn));

        kafka.deleteVpcConnection(b -> b.arn(vpcConnectionArn));
        assertThatThrownBy(() -> kafka.describeVpcConnection(b -> b.arn(vpcConnectionArn)))
                .isInstanceOf(NotFoundException.class);
        vpcConnectionArn = null;
    }
}
