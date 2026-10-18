package io.github.hectorvent.floci.services.msk;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/** The operations msk-black-shared and msk-vpc-connection drive: IAM listeners, policy, topics, VPC connections. */
@QuarkusTest
class MskIamAccessIntegrationTest {

    private static String createIamCluster(String name) {
        String clusterArn = given().contentType("application/json")
                .body("""
                    {"clusterName": "%s", "numberOfBrokerNodes": 3,
                     "brokerNodeGroupInfo": {"instanceType": "kafka.m7g.large", "clientSubnets": ["subnet-a"]},
                     "encryptionInfo": {"encryptionInTransit": {"clientBroker": "TLS", "inCluster": true}},
                     "clientAuthentication": {"sasl": {"iam": {"enabled": true}}}}
                    """.formatted(name))
            .when().post("/v1/clusters")
            .then().statusCode(200).extract().path("clusterArn");
        String version = given().when().get("/v1/clusters/{arn}", clusterArn)
            .then().statusCode(200).extract().path("clusterInfo.currentVersion");
        String operationArn = given().contentType("application/json")
                .body("""
                    {"currentVersion": "%s", "connectivityInfo": {"vpcConnectivity": {"clientAuthentication":
                     {"sasl": {"iam": {"enabled": true}}}}}}
                    """.formatted(version))
            .when().put("/v1/clusters/{arn}/connectivity", clusterArn)
            .then().statusCode(200).extract().path("clusterOperationArn");
        given().when().get("/v1/operations/{arn}", operationArn)
            .then().statusCode(200)
                .body("clusterOperationInfo.operationState", equalTo("UPDATE_COMPLETE"));
        return clusterArn;
    }

    @Test
    void bootstrapBrokersListTheIamListenersOfEveryBroker() {
        String clusterArn = createIamCluster("iam-listeners");

        given().when().get("/v1/clusters/{arn}/bootstrap-brokers", clusterArn)
            .then().statusCode(200)
                .body("$", not(hasKey("bootstrapBrokerStringTls")))
                .body("bootstrapBrokerStringSaslIam", matchesPattern(
                        "b-1\\.iamlisteners\\.[a-z0-9]{6}\\.c1\\.kafka\\.us-east-1\\.amazonaws\\.com:9098,"
                                + "b-2\\..*:9098,b-3\\..*:9098"))
                .body("bootstrapBrokerStringVpcConnectivitySaslIam", matchesPattern(
                        "b-1\\.iam\\.iamlisteners\\..*:14001,b-2\\.iam\\..*:14002,b-3\\.iam\\..*:14003"));
        given().when().get("/v1/clusters/{arn}", clusterArn)
            .then().statusCode(200)
                .body("clusterInfo.brokerNodeGroupInfo.connectivityInfo.vpcConnectivity.clientAuthentication.sasl.iam.enabled",
                        equalTo(true));
    }

    @Test
    void clusterPolicyRoundTripsWithItsVersion() {
        String clusterArn = createIamCluster("iam-policy");
        String policy = "{\"Version\":\"2012-10-17\",\"Statement\":[]}";

        String version = given().contentType("application/json")
                .body("{\"policy\": \"" + policy.replace("\"", "\\\"") + "\"}")
            .when().put("/v1/clusters/{arn}/policy", clusterArn)
            .then().statusCode(200).extract().path("currentVersion");
        given().when().get("/v1/clusters/{arn}/policy", clusterArn)
            .then().statusCode(200)
                .body("policy", equalTo(policy))
                .body("currentVersion", equalTo(version));
        given().when().delete("/v1/clusters/{arn}/policy", clusterArn).then().statusCode(200);
        given().when().get("/v1/clusters/{arn}/policy", clusterArn).then().statusCode(404);
    }

    @Test
    void topicLifecycle() {
        String clusterArn = createIamCluster("iam-topics");
        String configs = Base64.getEncoder().encodeToString("{\"retention.ms\":\"1000\"}".getBytes(StandardCharsets.UTF_8));

        given().contentType("application/json")
                .body("{\"topicName\": \"trade.order\", \"partitionCount\": 3, \"replicationFactor\": 3, "
                        + "\"configs\": \"" + configs + "\"}")
            .when().post("/v1/clusters/{arn}/topics", clusterArn)
            .then().statusCode(200)
                .body("topicArn", containsString(":topic/iam-topics/"));
        given().when().get("/v1/clusters/{arn}/topics/{name}", clusterArn, "trade.order")
            .then().statusCode(200)
                .body("partitionCount", equalTo(3))
                .body("replicationFactor", equalTo(3))
                .body("configs", equalTo(configs))
                .body("status", equalTo("ACTIVE"));
        given().contentType("application/json").body("{\"partitionCount\": 6}")
            .when().put("/v1/clusters/{arn}/topics/{name}", clusterArn, "trade.order")
            .then().statusCode(200);
        given().when().get("/v1/clusters/{arn}/topics", clusterArn)
            .then().statusCode(200)
                .body("topics.topicName", contains("trade.order"))
                .body("topics[0].partitionCount", equalTo(6));
        given().when().delete("/v1/clusters/{arn}/topics/{name}", clusterArn, "trade.order")
            .then().statusCode(200);
        given().when().get("/v1/clusters/{arn}/topics/{name}", clusterArn, "trade.order")
            .then().statusCode(404);
    }

    @Test
    void vpcConnectionLifecycle() {
        String clusterArn = createIamCluster("iam-vpc-connection");

        String connectionArn = given().contentType("application/json")
                .body("""
                    {"targetClusterArn": "%s", "authentication": "SASL_IAM", "vpcId": "vpc-1",
                     "clientSubnets": ["subnet-1", "subnet-2"], "securityGroups": ["sg-1"], "tags": {"team": "platform"}}
                    """.formatted(clusterArn))
            .when().post("/v1/vpc-connection")
            .then().statusCode(200)
                .body("state", equalTo("AVAILABLE"))
                .extract().path("vpcConnectionArn");
        given().when().get("/v1/vpc-connection/{arn}", connectionArn)
            .then().statusCode(200)
                .body("targetClusterArn", equalTo(clusterArn))
                .body("subnets", contains("subnet-1", "subnet-2"))
                .body("tags.team", equalTo("platform"))
                .body("creationTime", notNullValue());
        given().when().get("/v1/clusters/{arn}/client-vpc-connections", clusterArn)
            .then().statusCode(200)
                .body("clientVpcConnections.vpcConnectionArn", contains(connectionArn));
        given().when().delete("/v1/vpc-connection/{arn}", connectionArn).then().statusCode(200);
        given().when().get("/v1/vpc-connection/{arn}", connectionArn).then().statusCode(404);
    }
}
