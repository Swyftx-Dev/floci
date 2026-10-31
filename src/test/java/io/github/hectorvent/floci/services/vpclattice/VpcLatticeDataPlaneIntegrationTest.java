package io.github.hectorvent.floci.services.vpclattice;

import com.sun.net.httpserver.HttpServer;
import io.github.hectorvent.floci.config.FlociCertificateAuthority;
import io.github.hectorvent.floci.core.common.RequestScopes;
import io.github.hectorvent.floci.services.acm.AcmService;
import io.github.hectorvent.floci.services.acm.CertificateGenerator;
import io.github.hectorvent.floci.services.acm.model.KeyAlgorithm;
import io.github.hectorvent.floci.services.ec2.Ec2Service;
import io.github.hectorvent.floci.testing.RealVpcLatticeDataPlaneProfile;
import io.github.hectorvent.floci.testing.RestAssuredJsonUtils;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.specification.RequestSpecification;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(RealVpcLatticeDataPlaneProfile.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class VpcLatticeDataPlaneIntegrationTest {

    private static final String LATTICE_AUTH =
            "AWS4-HMAC-SHA256 Credential=333333333333/20261018/us-east-1/vpc-lattice/aws4_request";
    private static final String ELB_AUTH =
            "AWS4-HMAC-SHA256 Credential=333333333333/20261018/us-east-1/elasticloadbalancing/aws4_request";
    private static final int HTTP_PORT = 7811;
    private static final int HTTPS_PORT = 7812;

    private static HttpServer backend;
    private static String openHost;
    private static String iamHost;
    private static String iamTargetGroupId;
    private static String albDnsName;

    @Inject
    FlociCertificateAuthority certificateAuthority;

    @Inject
    AcmService acmService;

    @BeforeAll
    static void startBackend() throws IOException {
        RestAssuredJsonUtils.configureAwsContentTypes();
        backend = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        backend.createContext("/", exchange -> {
            byte[] body = ("backend " + exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath()
                    + " host=" + exchange.getRequestHeaders().getFirst("Host")).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        backend.start();
    }

    @AfterAll
    static void stopBackend() {
        backend.stop(0);
    }

    private static RequestSpecification lattice() {
        return given().header("Authorization", LATTICE_AUTH).contentType("application/json");
    }

    private static String createServiceWithListeners(String name, String authType) {
        String serviceId = lattice().body("{\"name\":\"" + name + "\",\"authType\":\"" + authType + "\"}")
            .when().post("/services")
            .then().statusCode(201).extract().path("id");
        String targetGroupId = lattice()
                .body("{\"name\":\"" + name + "-tg\",\"type\":\"IP\",\"config\":{\"port\":"
                        + backend.getAddress().getPort() + ",\"protocol\":\"HTTP\",\"vpcIdentifier\":\"vpc-dp\","
                        + "\"healthCheck\":{\"enabled\":true,\"path\":\"/healthcheck\",\"healthCheckIntervalSeconds\":5,"
                        + "\"healthyThresholdCount\":2}}}")
            .when().post("/targetgroups")
            .then().statusCode(201).extract().path("id");
        String forward = "{\"forward\":{\"targetGroups\":[{\"targetGroupIdentifier\":\"" + targetGroupId
                + "\",\"weight\":100}]}}";
        lattice().body("{\"name\":\"http\",\"protocol\":\"HTTP\",\"port\":" + HTTP_PORT + ",\"defaultAction\":"
                        + forward + "}")
            .when().post("/services/" + serviceId + "/listeners")
            .then().statusCode(201);
        lattice().body("{\"name\":\"https\",\"protocol\":\"HTTPS\",\"port\":" + HTTPS_PORT + ",\"defaultAction\":"
                        + forward + "}")
            .when().post("/services/" + serviceId + "/listeners")
            .then().statusCode(201);
        lattice().body("{\"targets\":[{\"id\":\"127.0.0.1\"}]}")
            .when().post("/targetgroups/" + targetGroupId + "/registertargets")
            .then().statusCode(200);
        if ("AWS_IAM".equals(authType)) {
            iamTargetGroupId = targetGroupId;
        }
        return lattice().when().get("/services/" + serviceId).then().extract().path("dnsEntry.domainName");
    }

    private static RequestSpecification listener(String host) {
        return given().baseUri("http://127.0.0.1").port(HTTP_PORT).header("Host", host);
    }

    @Test
    @Order(1)
    void forwardsByHostHeaderToTheRegisteredTarget() {
        openHost = createServiceWithListeners("open-svc", "NONE");
        iamHost = createServiceWithListeners("iam-svc", "AWS_IAM");

        listener(openHost).when().get("/hello")
            .then().statusCode(200)
                .body(equalTo("backend GET /hello host=" + openHost));
        listener(openHost).body("payload").when().post("/submit")
            .then().statusCode(200)
                .body(containsString("backend POST /submit"));
    }

    @Test
    @Order(2)
    void anAwsIamServiceRefusesUnsignedRequests() {
        listener(iamHost).when().get("/")
            .then().statusCode(403)
                .body(containsString("AccessDeniedException"));
        listener(iamHost)
                .header("Authorization", "AWS4-HMAC-SHA256 Credential=333333333333/20261018/us-east-1/"
                        + "vpc-lattice-svcs/aws4_request, SignedHeaders=host, Signature=abc")
            .when().get("/signed")
            .then().statusCode(200)
                .body(containsString("backend GET /signed"));
    }

    @Test
    @Order(3)
    void healthChecksMarkTheTargetHealthy() {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                lattice().body("{}")
                    .when().post("/targetgroups/" + iamTargetGroupId + "/listtargets")
                    .then().statusCode(200)
                        .body("items[0].status", equalTo("HEALTHY")));
    }

    @Test
    @Order(4)
    void sharesItsPortWithALoadBalancer() {
        String lbArn = given()
                .formParam("Action", "CreateLoadBalancer")
                .formParam("Name", "lattice-neighbour")
                .formParam("Type", "application")
                .formParam("Subnets.member.1", Ec2Service.defaultSubnetId("us-east-1", "a"))
                .formParam("Subnets.member.2", Ec2Service.defaultSubnetId("us-east-1", "b"))
                .header("Authorization", ELB_AUTH)
            .when().post("/")
            .then().statusCode(200)
                .extract().path("CreateLoadBalancerResponse.CreateLoadBalancerResult.LoadBalancers.member.LoadBalancerArn");
        albDnsName = given()
                .formParam("Action", "DescribeLoadBalancers")
                .formParam("LoadBalancerArns.member.1", lbArn)
                .header("Authorization", ELB_AUTH)
            .when().post("/")
            .then().statusCode(200)
                .extract().path("DescribeLoadBalancersResponse.DescribeLoadBalancersResult.LoadBalancers.member.DNSName");
        given()
                .formParam("Action", "CreateListener")
                .formParam("LoadBalancerArn", lbArn)
                .formParam("Protocol", "HTTP")
                .formParam("Port", String.valueOf(HTTP_PORT))
                .formParam("DefaultActions.member.1.Type", "fixed-response")
                .formParam("DefaultActions.member.1.FixedResponseConfig.StatusCode", "200")
                .formParam("DefaultActions.member.1.FixedResponseConfig.ContentType", "text/plain")
                .formParam("DefaultActions.member.1.FixedResponseConfig.MessageBody", "alb")
                .header("Authorization", ELB_AUTH)
            .when().post("/")
            .then().statusCode(200);

        listener(albDnsName).when().get("/").then().statusCode(200).body(equalTo("alb"));
        listener("unknown.example.test").when().get("/").then().statusCode(200).body(equalTo("alb"));
        listener(openHost).when().get("/still-lattice")
            .then().statusCode(200)
                .body(containsString("backend GET /still-lattice"));
    }

    @Test
    @Order(5)
    void httpsListenerPresentsACertificateForTheGeneratedName() throws Exception {
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, trustingFlociCa().getTrustManagers(), null);
        try (SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket("127.0.0.1", HTTPS_PORT)) {
            SSLParameters parameters = socket.getSSLParameters();
            parameters.setServerNames(List.of(new SNIHostName(openHost)));
            parameters.setEndpointIdentificationAlgorithm("HTTPS");
            socket.setSSLParameters(parameters);
            socket.startHandshake();
            X509Certificate leaf = (X509Certificate) socket.getSession().getPeerCertificates()[0];
            assertTrue(leaf.getSubjectAlternativeNames().stream().anyMatch(san -> openHost.equals(san.get(1))));

            socket.getOutputStream().write(("GET /tls HTTP/1.1\r\nHost: " + openHost
                    + "\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
            String response = new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            assertTrue(response.contains("backend GET /tls"), response);
        }
    }

    @Test
    @Order(6)
    void sharesItsTlsPortWithAnHttpsLoadBalancerBySni() throws Exception {
        String lbArn = given()
                .formParam("Action", "CreateLoadBalancer")
                .formParam("Name", "lattice-tls-neighbour")
                .formParam("Type", "application")
                .formParam("Subnets.member.1", Ec2Service.defaultSubnetId("us-east-1", "a"))
                .formParam("Subnets.member.2", Ec2Service.defaultSubnetId("us-east-1", "b"))
                .header("Authorization", ELB_AUTH)
            .when().post("/")
            .then().statusCode(200)
                .extract().path("CreateLoadBalancerResponse.CreateLoadBalancerResult.LoadBalancers.member.LoadBalancerArn");
        String dnsName = given()
                .formParam("Action", "DescribeLoadBalancers")
                .formParam("LoadBalancerArns.member.1", lbArn)
                .header("Authorization", ELB_AUTH)
            .when().post("/")
            .then().statusCode(200)
                .extract().path("DescribeLoadBalancersResponse.DescribeLoadBalancersResult.LoadBalancers.member.DNSName");
        CertificateGenerator.GeneratedCertificate pair = new CertificateGenerator().generateSelfSignedCertificate(
                dnsName, List.of(), KeyAlgorithm.RSA_2048);
        String[] certificateArn = new String[1];
        RequestScopes.runAs("333333333333", () -> certificateArn[0] = acmService.importCertificate(
                pair.certificatePem(), pair.privateKeyPem(), null, null, Map.of(), "us-east-1").getArn());
        given()
                .formParam("Action", "CreateListener")
                .formParam("LoadBalancerArn", lbArn)
                .formParam("Protocol", "HTTPS")
                .formParam("Port", String.valueOf(HTTPS_PORT))
                .formParam("Certificates.member.1.CertificateArn", certificateArn[0])
                .formParam("DefaultActions.member.1.Type", "fixed-response")
                .formParam("DefaultActions.member.1.FixedResponseConfig.StatusCode", "200")
                .formParam("DefaultActions.member.1.FixedResponseConfig.ContentType", "text/plain")
                .formParam("DefaultActions.member.1.FixedResponseConfig.MessageBody", "alb-tls")
                .header("Authorization", ELB_AUTH)
            .when().post("/")
            .then().statusCode(200);

        SSLContext trustAll = SSLContext.getInstance("TLS");
        trustAll.init(null, new TrustManager[] {new TrustingTrustManager()}, null);
        String albResponse = null;
        for (int attempt = 0; attempt < 50 && (albResponse == null || !albResponse.contains("alb-tls")); attempt++) {
            Thread.sleep(attempt == 0 ? 0 : 100);
            albResponse = tlsGet(trustAll, dnsName);
        }
        assertTrue(albResponse != null && albResponse.contains("alb-tls"), String.valueOf(albResponse));

        SSLContext trustingFloci = SSLContext.getInstance("TLS");
        trustingFloci.init(null, trustingFlociCa().getTrustManagers(), null);
        String latticeResponse = tlsGet(trustingFloci, openHost);
        assertTrue(latticeResponse.contains("backend GET /tls"), latticeResponse);
    }

    /** One HTTPS GET of {@code /tls} on the shared port, naming {@code host} in SNI and Host. */
    private static String tlsGet(SSLContext context, String host) {
        try (SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket("127.0.0.1", HTTPS_PORT)) {
            SSLParameters parameters = socket.getSSLParameters();
            parameters.setServerNames(List.of(new SNIHostName(host)));
            socket.setSSLParameters(parameters);
            socket.setSoTimeout(5000);
            socket.getOutputStream().write(("GET /tls HTTP/1.1\r\nHost: " + host
                    + "\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
            return new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException expected) {
            // The listener may still be binding; the caller retries.
            return null;
        }
    }

    /** The load balancer's certificate is self-signed; this test is about routing, not trust. */
    private static final class TrustingTrustManager implements X509TrustManager {

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }

    private TrustManagerFactory trustingFlociCa() throws Exception {
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        trustStore.setCertificateEntry("floci", CertificateFactory.getInstance("X.509").generateCertificate(
                new ByteArrayInputStream(certificateAuthority.caPem().getBytes(StandardCharsets.UTF_8))));
        TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        factory.init(trustStore);
        return factory;
    }
}
