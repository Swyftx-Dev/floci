package io.github.hectorvent.floci.services.ecs;

import io.github.hectorvent.floci.testing.RestAssuredJsonUtils;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.response.Response;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DescribeServices reports a service's {@code vpcLatticeConfigurations} on its primary deployment,
 * where AWS's model puts them and Terraform reads them, and not on the service itself.
 */
@QuarkusTest
class EcsVpcLatticeConfigurationIntegrationTest {

    private static final String TARGET = "AmazonEC2ContainerServiceV20141113.";
    private static final String CT = "application/x-amz-json-1.1";
    private static final String CLUSTER = "lattice-cluster";
    private static final String FAMILY = "lattice-fam";
    private static final String CONFIG = "[{\"roleArn\":\"arn:aws:iam::000000000000:role/ecs-infra\","
            + "\"targetGroupArn\":\"arn:aws:vpc-lattice:us-east-1:000000000000:targetgroup/tg-0123456789abcdef0\","
            + "\"portName\":\"http\"}]";

    @BeforeAll
    static void configure() {
        RestAssuredJsonUtils.configureAwsContentTypes();
    }

    private static Response call(String action, String body) {
        return given().contentType(CT).header("X-Amz-Target", TARGET + action)
                .body(body)
                .when().post("/")
                .then().statusCode(200)
                .extract().response();
    }

    private static Response createAndDescribe(String name) {
        call("CreateCluster", "{\"clusterName\":\"" + CLUSTER + "\"}");
        call("RegisterTaskDefinition", "{\"family\":\"" + FAMILY + "\",\"containerDefinitions\":[{\"name\":\"web\","
                + "\"image\":\"nginx\",\"memory\":128,\"portMappings\":[{\"name\":\"http\",\"containerPort\":80}]}]}");
        call("CreateService", "{\"cluster\":\"" + CLUSTER + "\",\"serviceName\":\"" + name + "\","
                + "\"taskDefinition\":\"" + FAMILY + "\",\"desiredCount\":0,\"vpcLatticeConfigurations\":" + CONFIG + "}");
        return describe(name);
    }

    private static Response describe(String name) {
        return call("DescribeServices", "{\"cluster\":\"" + CLUSTER + "\",\"services\":[\"" + name + "\"]}");
    }

    @Test
    void createService_reportsTheConfigurationsOnThePrimaryDeployment() {
        Response described = createAndDescribe("lattice-create");
        String base = "services[0].deployments[0].vpcLatticeConfigurations[0].";

        assertEquals("PRIMARY", described.jsonPath().getString("services[0].deployments[0].status"));
        assertEquals("http", described.jsonPath().getString(base + "portName"));
        assertTrue(described.jsonPath().getString(base + "targetGroupArn").endsWith("tg-0123456789abcdef0"));
        assertEquals("arn:aws:iam::000000000000:role/ecs-infra", described.jsonPath().getString(base + "roleArn"));
        assertNull(described.jsonPath().get("services[0].vpcLatticeConfigurations"));
    }

    @Test
    void updateService_withAnEmptyListClearsTheConfigurations() {
        createAndDescribe("lattice-clear");
        call("UpdateService", "{\"cluster\":\"" + CLUSTER + "\",\"service\":\"lattice-clear\","
                + "\"vpcLatticeConfigurations\":[]}");

        assertTrue(describe("lattice-clear").jsonPath()
                .getList("services[0].deployments[0].vpcLatticeConfigurations").isEmpty());
    }
}
