package io.github.hectorvent.floci.services.vpclattice;

import io.github.hectorvent.floci.testing.RestAssuredJsonUtils;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.specification.RequestSpecification;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class VpcLatticeIntegrationTest {

    private static final String NETWORK_OWNER = "111111111111";
    private static final String SERVICE_OWNER = "222222222222";

    private static String serviceNetworkId;
    private static String serviceNetworkArn;
    private static String serviceId;
    private static String serviceArn;
    private static String targetGroupId;
    private static String targetGroupArn;
    private static String listenerId;
    private static String serviceAssociationId;
    private static String vpcAssociationId;
    private static String accessLogSubscriptionId;

    @BeforeAll
    static void configureRestAssured() {
        RestAssuredJsonUtils.configureAwsContentTypes();
    }

    private static RequestSpecification as(String account) {
        return given()
                .header("Authorization", "AWS4-HMAC-SHA256 Credential=" + account
                        + "/20261018/ap-southeast-2/vpc-lattice/aws4_request")
                .contentType("application/json");
    }

    private static String encoded(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    @Test
    @Order(1)
    void createServiceNetwork() {
        serviceNetworkId = as(NETWORK_OWNER)
                .body("{\"name\":\"tst-vpc-lattice-apse2\",\"authType\":\"AWS_IAM\",\"tags\":{\"Name\":\"sn\"}}")
            .when().post("/servicenetworks")
            .then().statusCode(201)
                .body("id", startsWith("sn-"))
                .body("arn", startsWith("arn:aws:vpc-lattice:ap-southeast-2:" + NETWORK_OWNER + ":servicenetwork/sn-"))
                .body("authType", equalTo("AWS_IAM"))
                .extract().path("id");
        serviceNetworkArn = as(NETWORK_OWNER).when().get("/servicenetworks/" + serviceNetworkId)
            .then().statusCode(200)
                .body("name", equalTo("tst-vpc-lattice-apse2"))
                .body("numberOfAssociatedVPCs", equalTo(0))
                .body("createdAt", notNullValue())
                .extract().path("arn");
    }

    @Test
    @Order(2)
    void createServiceWithCustomDomain() {
        serviceId = as(SERVICE_OWNER)
                .body("{\"name\":\"ecs-echo\",\"authType\":\"AWS_IAM\","
                        + "\"customDomainName\":\"echo.test.example.lattice.internal\","
                        + "\"certificateArn\":\"arn:aws:acm:ap-southeast-2:" + SERVICE_OWNER + ":certificate/abc\"}")
            .when().post("/services")
            .then().statusCode(201)
                .body("status", equalTo("ACTIVE"))
                .body("customDomainName", equalTo("echo.test.example.lattice.internal"))
                .body("dnsEntry.domainName", startsWith("ecs-echo-"))
                .body("dnsEntry.domainName", containsString(".vpc-lattice-svcs.ap-southeast-2."))
                .body("dnsEntry.hostedZoneId", startsWith("Z"))
                .extract().path("id");
        serviceArn = as(SERVICE_OWNER).when().get("/services/" + serviceId)
            .then().statusCode(200)
                .body("authType", equalTo("AWS_IAM"))
                .body("$", not(hasKey("idleTimeoutSeconds")))
                .extract().path("arn");
    }

    @Test
    @Order(3)
    void duplicateServiceNameConflicts() {
        as(SERVICE_OWNER).body("{\"name\":\"ecs-echo\"}")
            .when().post("/services")
            .then().statusCode(409)
                .header("X-Amzn-Errortype", "ConflictException");
    }

    @Test
    @Order(4)
    void createTargetGroupRoundTripsItsConfig() {
        targetGroupId = as(SERVICE_OWNER)
                .body("{\"name\":\"ecs-echo-3800\",\"type\":\"IP\",\"config\":{\"port\":3800,\"protocol\":\"HTTP\","
                        + "\"protocolVersion\":\"HTTP1\",\"ipAddressType\":\"IPV4\",\"vpcIdentifier\":\"vpc-123\","
                        + "\"healthCheck\":{\"enabled\":true,\"protocol\":\"HTTP\",\"protocolVersion\":\"HTTP1\","
                        + "\"path\":\"/healthcheck\",\"healthCheckIntervalSeconds\":10,\"healthCheckTimeoutSeconds\":5,"
                        + "\"healthyThresholdCount\":2,\"unhealthyThresholdCount\":3,\"matcher\":{\"httpCode\":\"200\"}}}}")
            .when().post("/targetgroups")
            .then().statusCode(201)
                .body("type", equalTo("IP"))
                .body("config.healthCheck.path", equalTo("/healthcheck"))
                .body("config.healthCheck.matcher.httpCode", equalTo("200"))
                .body("config.healthCheck", not(hasKey("port")))
                .extract().path("id");
        targetGroupArn = as(SERVICE_OWNER).when().get("/targetgroups/" + targetGroupId)
            .then().statusCode(200)
                .body("config.port", equalTo(3800))
                .body("config.vpcIdentifier", equalTo("vpc-123"))
                .body("serviceArns.size()", equalTo(0))
                .extract().path("arn");
    }

    @Test
    @Order(5)
    void createHttpsListenerForwardingToTheTargetGroup() {
        listenerId = as(SERVICE_OWNER)
                .body("{\"name\":\"https\",\"protocol\":\"HTTPS\",\"port\":443,\"defaultAction\":{\"forward\":"
                        + "{\"targetGroups\":[{\"targetGroupIdentifier\":\"" + targetGroupId + "\",\"weight\":100}]}}}")
            .when().post("/services/" + serviceId + "/listeners")
            .then().statusCode(201)
                .body("serviceId", equalTo(serviceId))
                .body("defaultAction.forward.targetGroups[0].targetGroupIdentifier", equalTo(targetGroupId))
                .body("defaultAction.forward.targetGroups[0].weight", equalTo(100))
                .body("arn", startsWith(serviceArn + "/listener/listener-"))
                .extract().path("id");
        as(SERVICE_OWNER).when().get("/services/" + serviceId + "/listeners/" + listenerId)
            .then().statusCode(200)
                .body("port", equalTo(443))
                .body("createdAt", notNullValue());
        as(SERVICE_OWNER).when().get("/targetgroups/" + targetGroupId)
            .then().statusCode(200)
                .body("serviceArns", contains(serviceArn));
    }

    @Test
    @Order(6)
    void aTargetGroupInUseCannotBeDeleted() {
        as(SERVICE_OWNER).when().delete("/targetgroups/" + targetGroupId)
            .then().statusCode(409)
                .header("X-Amzn-Errortype", "ConflictException");
    }

    @Test
    @Order(7)
    void registerListAndDeregisterTargets() {
        as(SERVICE_OWNER).body("{\"targets\":[{\"id\":\"10.0.0.5\",\"port\":3800},{\"id\":\"10.0.0.6\"}]}")
            .when().post("/targetgroups/" + targetGroupId + "/registertargets")
            .then().statusCode(200)
                .body("successful.id", contains("10.0.0.5", "10.0.0.6"))
                .body("successful[1].port", equalTo(3800))
                .body("unsuccessful.size()", equalTo(0));
        as(SERVICE_OWNER).body("{}")
            .when().post("/targetgroups/" + targetGroupId + "/listtargets")
            .then().statusCode(200)
                .body("items.size()", equalTo(2))
                .body("items[0].status", equalTo("INITIAL"));
        as(SERVICE_OWNER).body("{\"targets\":[{\"id\":\"10.0.0.6\",\"port\":3800}]}")
            .when().post("/targetgroups/" + targetGroupArn.replace("/", "%2F").replace(":", "%3A")
                    + "/deregistertargets")
            .then().statusCode(200);
        as(SERVICE_OWNER).body("{}")
            .when().post("/targetgroups/" + targetGroupId + "/listtargets")
            .then().statusCode(200)
                .body("items.id", contains("10.0.0.5"));
    }

    @Test
    @Order(8)
    void associateTheServiceWithANetworkInAnotherAccount() {
        serviceAssociationId = as(SERVICE_OWNER)
                .body("{\"serviceIdentifier\":\"" + serviceId + "\",\"serviceNetworkIdentifier\":\""
                        + serviceNetworkArn + "\"}")
            .when().post("/servicenetworkserviceassociations")
            .then().statusCode(200)
                .body("status", equalTo("ACTIVE"))
                .body("createdBy", equalTo(SERVICE_OWNER))
                .body("customDomainName", equalTo("echo.test.example.lattice.internal"))
                .extract().path("id");
        as(SERVICE_OWNER).when().get("/servicenetworkserviceassociations/" + serviceAssociationId)
            .then().statusCode(200)
                .body("serviceId", equalTo(serviceId))
                .body("serviceNetworkId", equalTo(serviceNetworkId))
                .body("dnsEntry.domainName", startsWith("ecs-echo-"));
        as(NETWORK_OWNER).queryParam("serviceNetworkIdentifier", serviceNetworkId)
            .when().get("/servicenetworkserviceassociations")
            .then().statusCode(200)
                .body("items.id", hasItem(serviceAssociationId));
        as(NETWORK_OWNER).when().get("/servicenetworks/" + serviceNetworkId)
            .then().statusCode(200)
                .body("numberOfAssociatedServices", equalTo(1));
    }

    @Test
    @Order(9)
    void associateAVpcWithPrivateDnsOptions() {
        vpcAssociationId = as(SERVICE_OWNER)
                .body("{\"serviceNetworkIdentifier\":\"" + serviceNetworkArn + "\",\"vpcIdentifier\":\"vpc-123\","
                        + "\"privateDnsEnabled\":true,\"securityGroupIds\":[\"sg-1\"],\"dnsOptions\":{"
                        + "\"privateDnsPreference\":\"SPECIFIED_DOMAINS_ONLY\","
                        + "\"privateDnsSpecifiedDomains\":[\"*.test.example.lattice.internal\"]}}")
            .when().post("/servicenetworkvpcassociations")
            .then().statusCode(200)
                .body("securityGroupIds", contains("sg-1"))
                .extract().path("id");
        as(SERVICE_OWNER).when().get("/servicenetworkvpcassociations/" + vpcAssociationId)
            .then().statusCode(200)
                .body("vpcId", equalTo("vpc-123"))
                .body("serviceNetworkId", equalTo(serviceNetworkId))
                .body("privateDnsEnabled", equalTo(true))
                .body("dnsOptions.privateDnsPreference", equalTo("SPECIFIED_DOMAINS_ONLY"))
                .body("dnsOptions.privateDnsSpecifiedDomains", contains("*.test.example.lattice.internal"));
    }

    @Test
    @Order(10)
    void authPolicyRoundTripsByServiceArn() {
        String policy = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":\"*\","
                + "\"Action\":\"vpc-lattice-svcs:Invoke\",\"Resource\":\"*\"}]}";
        as(SERVICE_OWNER).body("{\"policy\":" + quoted(policy) + "}")
            .when().put("/authpolicy/" + encoded(serviceArn))
            .then().statusCode(200)
                .body("state", equalTo("Active"));
        as(SERVICE_OWNER).when().get("/authpolicy/" + encoded(serviceArn))
            .then().statusCode(200)
                .body("policy", equalTo(policy));
        as(SERVICE_OWNER).when().delete("/authpolicy/" + encoded(serviceArn))
            .then().statusCode(204);
        as(SERVICE_OWNER).when().get("/authpolicy/" + encoded(serviceArn))
            .then().statusCode(404)
                .header("X-Amzn-Errortype", "ResourceNotFoundException");
    }

    private static String quoted(String json) {
        return "\"" + json.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    @Test
    @Order(11)
    void accessLogSubscriptionIsStored() {
        accessLogSubscriptionId = as(SERVICE_OWNER)
                .body("{\"resourceIdentifier\":\"" + serviceArn + "\",\"destinationArn\":"
                        + "\"arn:aws:logs:ap-southeast-2:" + SERVICE_OWNER + ":log-group:/aws/vpc-lattice/services\"}")
            .when().post("/accesslogsubscriptions")
            .then().statusCode(201)
                .body("resourceId", equalTo(serviceId))
                .body("resourceArn", equalTo(serviceArn))
                .extract().path("id");
        as(SERVICE_OWNER).queryParam("resourceIdentifier", serviceId)
            .when().get("/accesslogsubscriptions")
            .then().statusCode(200)
                .body("items.id", contains(accessLogSubscriptionId))
                .body("items[0].serviceNetworkLogType", nullValue());
    }

    @Test
    @Order(12)
    void tagsRoundTripByEncodedArn() {
        as(SERVICE_OWNER).body("{\"tags\":{\"Project\":\"lattice\",\"Team\":\"platform\"}}")
            .when().post("/tags/" + encoded(serviceArn))
            .then().statusCode(200);
        as(SERVICE_OWNER).queryParam("tagKeys", "Team")
            .when().delete("/tags/" + encoded(serviceArn))
            .then().statusCode(200);
        as(SERVICE_OWNER).when().get("/tags/" + encoded(serviceArn))
            .then().statusCode(200)
                .body("tags.Project", equalTo("lattice"))
                .body("tags", not(hasKey("Team")));
        as(NETWORK_OWNER).when().get("/tags/" + encoded(serviceNetworkArn))
            .then().statusCode(200)
                .body("tags.Name", equalTo("sn"));
    }

    @Test
    @Order(13)
    void aServiceInANetworkCannotBeDeletedUntilDisassociated() {
        as(SERVICE_OWNER).when().delete("/services/" + serviceId)
            .then().statusCode(409);
        as(SERVICE_OWNER).when().delete("/servicenetworkserviceassociations/" + serviceAssociationId)
            .then().statusCode(200)
                .body("status", equalTo("DELETE_IN_PROGRESS"));
        as(SERVICE_OWNER).when().delete("/servicenetworkvpcassociations/" + vpcAssociationId)
            .then().statusCode(200);
    }

    @Test
    @Order(14)
    void deletingTheServiceTakesItsListenerAndLogSubscription() {
        as(SERVICE_OWNER).when().delete("/services/" + serviceId)
            .then().statusCode(200)
                .body("status", equalTo("DELETE_IN_PROGRESS"));
        as(SERVICE_OWNER).when().get("/services/" + serviceId)
            .then().statusCode(404);
        as(SERVICE_OWNER).when().get("/accesslogsubscriptions/" + accessLogSubscriptionId)
            .then().statusCode(404);
        as(SERVICE_OWNER).when().delete("/targetgroups/" + targetGroupId)
            .then().statusCode(200);
        as(NETWORK_OWNER).when().delete("/servicenetworks/" + serviceNetworkId)
            .then().statusCode(204);
    }

    @Test
    @Order(15)
    void anUnsignedPathStaysWithS3() {
        given().when().get("/servicenetworks")
            .then().body(not(containsString("\"items\"")));
        as(NETWORK_OWNER).when().get("/servicenetworks")
            .then().statusCode(200)
                .body("items.arn", not(hasItem(endsWith(String.valueOf(serviceNetworkId)))));
    }
}
