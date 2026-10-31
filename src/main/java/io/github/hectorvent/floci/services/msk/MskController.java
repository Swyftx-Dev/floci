package io.github.hectorvent.floci.services.msk;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.PaginatedResult;
import io.github.hectorvent.floci.core.common.Pagination;
import io.github.hectorvent.floci.services.msk.model.ConfigurationRevision;
import io.github.hectorvent.floci.services.msk.model.ConfigurationRevisionDetail;
import io.github.hectorvent.floci.services.msk.model.ConnectivityInfo;
import io.github.hectorvent.floci.services.msk.model.CreateClusterRequest;
import io.github.hectorvent.floci.services.msk.model.CreateClusterV2Request;
import io.github.hectorvent.floci.services.msk.model.MskCluster;
import io.github.hectorvent.floci.services.msk.model.MskConfiguration;
import io.github.hectorvent.floci.services.msk.model.MskTopic;
import io.github.hectorvent.floci.services.msk.model.MskVpcConnection;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MskController {

    // Fallback for clusters persisted before clusterType was stored - everything written back
    // then was provisioned, since serverless had no representation at all.
    private static final String PROVISIONED_CLUSTER_TYPE = "PROVISIONED";
    private static final ObjectMapper JSON = new ObjectMapper();

    private final MskService mskService;
    private final MskScramSecretsService mskScramSecretsService;

    @Inject
    public MskController(MskService mskService, MskScramSecretsService mskScramSecretsService) {
        this.mskService = mskService;
        this.mskScramSecretsService = mskScramSecretsService;
    }

    @POST
    @Path("/v1/clusters")
    public Response createCluster(CreateClusterRequest request) {
        MskCluster cluster = mskService.createCluster(request);
        return Response.ok(Map.of("clusterArn", cluster.getClusterArn(), "clusterName", cluster.getClusterName(), "state", cluster.getState())).build();
    }

    @POST
    @Path("/api/v2/clusters")
    public Response createClusterV2(CreateClusterV2Request request) {
        MskCluster cluster = mskService.createCluster(request);
        return Response.ok(Map.of(
                "clusterArn", cluster.getClusterArn(),
                "clusterName", cluster.getClusterName(),
                "state", cluster.getState(),
                "clusterType", clusterType(cluster))).build();
    }

    @GET
    @Path("/v1/clusters")
    public Response listClusters() {
        List<Map<String, Object>> clusters = mskService.listProvisionedClusters().stream().map(this::toClusterViewV1).toList();
        return Response.ok(Map.of("clusterInfoList", clusters)).build();
    }

    @GET
    @Path("/api/v2/clusters")
    public Response listClustersV2() {
        List<Map<String, Object>> clusters = mskService.listClusters().stream().map(this::toClusterViewV2).toList();
        return Response.ok(Map.of("clusterInfoList", clusters)).build();
    }

    @GET
    @Path("/v1/clusters/{clusterArn}")
    public Response describeCluster(@PathParam("clusterArn") String clusterArn) {
        MskCluster cluster = mskService.describeClusterV1(clusterArn);
        return Response.ok(Map.of("clusterInfo", toClusterViewV1(cluster))).build();
    }

    @GET
    @Path("/api/v2/clusters/{clusterArn}")
    public Response describeClusterV2(@PathParam("clusterArn") String clusterArn) {
        MskCluster cluster = mskService.describeCluster(clusterArn);
        return Response.ok(Map.of("clusterInfo", toClusterViewV2(cluster))).build();
    }

    @DELETE
    @Path("/v1/clusters/{clusterArn}")
    public Response deleteCluster(@PathParam("clusterArn") String clusterArn) {
        mskService.deleteCluster(clusterArn);
        return Response.ok(Map.of("clusterArn", clusterArn, "state", "DELETING")).build();
    }

    @GET
    @Path("/v1/clusters/{clusterArn}/bootstrap-brokers")
    public Response getBootstrapBrokers(@PathParam("clusterArn") String clusterArn) {
        return Response.ok(mskService.bootstrapBrokerStrings(clusterArn)).build();
    }

    @PUT
    @Path("/v1/clusters/{clusterArn}/connectivity")
    public Response updateConnectivity(@PathParam("clusterArn") String clusterArn, Map<String, Object> request) {
        ConnectivityInfo connectivity;
        try {
            connectivity = request.get("connectivityInfo") == null ? null
                    : JSON.convertValue(request.get("connectivityInfo"), ConnectivityInfo.class);
        } catch (IllegalArgumentException e) {
            throw new AwsException("BadRequestException", "connectivityInfo is not a ConnectivityInfo.", 400);
        }
        String operationArn = mskService.updateConnectivity(clusterArn, connectivity,
                asString(request.get("currentVersion"), "currentVersion"));
        return Response.ok(Map.of("clusterArn", clusterArn, "clusterOperationArn", operationArn)).build();
    }

    @GET
    @Path("/v1/operations/{clusterOperationArn}")
    public Response describeClusterOperation(@PathParam("clusterOperationArn") String operationArn) {
        return Response.ok(Map.of("clusterOperationInfo", mskService.describeClusterOperation(operationArn))).build();
    }

    @PUT
    @Path("/v1/clusters/{clusterArn}/policy")
    public Response putClusterPolicy(@PathParam("clusterArn") String clusterArn, Map<String, Object> request) {
        String version = mskService.putClusterPolicy(clusterArn, asString(request.get("policy"), "policy"),
                asString(request.get("currentVersion"), "currentVersion"));
        return Response.ok(Map.of("currentVersion", version)).build();
    }

    @GET
    @Path("/v1/clusters/{clusterArn}/policy")
    public Response getClusterPolicy(@PathParam("clusterArn") String clusterArn) {
        MskCluster cluster = mskService.getClusterPolicy(clusterArn);
        return Response.ok(Map.of("policy", cluster.getPolicy(), "currentVersion", cluster.getPolicyVersion())).build();
    }

    @DELETE
    @Path("/v1/clusters/{clusterArn}/policy")
    public Response deleteClusterPolicy(@PathParam("clusterArn") String clusterArn) {
        mskService.deleteClusterPolicy(clusterArn);
        return Response.ok(Map.of()).build();
    }

    @POST
    @Path("/v1/vpc-connection")
    @SuppressWarnings("unchecked")
    public Response createVpcConnection(Map<String, Object> request) {
        MskVpcConnection connection = mskService.createVpcConnection(
                asString(request.get("targetClusterArn"), "targetClusterArn"),
                asString(request.get("authentication"), "authentication"),
                asString(request.get("vpcId"), "vpcId"),
                asStringList(request.get("clientSubnets"), "clientSubnets"),
                asStringList(request.get("securityGroups"), "securityGroups"),
                request.get("tags") instanceof Map<?, ?> tags ? (Map<String, String>) tags : null);
        Map<String, Object> view = new HashMap<>();
        view.put("vpcConnectionArn", connection.getVpcConnectionArn());
        view.put("state", connection.getState());
        view.put("authentication", connection.getAuthentication());
        view.put("vpcId", connection.getVpcId());
        view.put("clientSubnets", connection.getClientSubnets());
        view.put("securityGroups", connection.getSecurityGroups());
        view.put("creationTime", connection.getCreationTime().toString());
        view.put("tags", connection.getTags());
        return Response.ok(view).build();
    }

    @GET
    @Path("/v1/vpc-connection/{arn}")
    public Response describeVpcConnection(@PathParam("arn") String arn) {
        MskVpcConnection connection = mskService.describeVpcConnection(arn);
        Map<String, Object> view = vpcConnectionView(connection);
        view.put("subnets", connection.getClientSubnets());
        view.put("securityGroups", connection.getSecurityGroups());
        view.put("tags", connection.getTags());
        return Response.ok(view).build();
    }

    @GET
    @Path("/v1/vpc-connections")
    public Response listVpcConnections() {
        return Response.ok(Map.of("vpcConnections",
                mskService.listVpcConnections().stream().map(this::vpcConnectionView).toList())).build();
    }

    @GET
    @Path("/v1/clusters/{clusterArn}/client-vpc-connections")
    public Response listClientVpcConnections(@PathParam("clusterArn") String clusterArn) {
        List<Map<String, Object>> connections = mskService.listClientVpcConnections(clusterArn).stream()
                .map(c -> Map.<String, Object>of("vpcConnectionArn", c.getVpcConnectionArn(),
                        "authentication", c.getAuthentication(), "state", c.getState(),
                        "creationTime", c.getCreationTime().toString(), "owner", c.getAccountId()))
                .toList();
        return Response.ok(Map.of("clientVpcConnections", connections)).build();
    }

    @DELETE
    @Path("/v1/vpc-connection/{arn}")
    public Response deleteVpcConnection(@PathParam("arn") String arn) {
        MskVpcConnection connection = mskService.deleteVpcConnection(arn);
        return Response.ok(Map.of("vpcConnectionArn", connection.getVpcConnectionArn(),
                "state", connection.getState())).build();
    }

    @POST
    @Path("/v1/clusters/{clusterArn}/topics")
    public Response createTopic(@PathParam("clusterArn") String clusterArn, Map<String, Object> request) {
        MskTopic topic = mskService.createTopic(clusterArn, asString(request.get("topicName"), "topicName"),
                asInteger(request.get("partitionCount"), "partitionCount"),
                asInteger(request.get("replicationFactor"), "replicationFactor"),
                asString(request.get("configs"), "configs"));
        return Response.ok(Map.of("topicArn", topic.getTopicArn(), "topicName", topic.getTopicName(),
                "status", "ACTIVE")).build();
    }

    @GET
    @Path("/v1/clusters/{clusterArn}/topics")
    public Response listTopics(@PathParam("clusterArn") String clusterArn,
                               @QueryParam("topicNameFilter") String topicNameFilter) {
        List<Map<String, Object>> topics = mskService.listTopics(clusterArn, topicNameFilter).stream()
                .map(t -> Map.<String, Object>of("topicArn", t.getTopicArn(), "topicName", t.getTopicName(),
                        "partitionCount", t.getPartitionCount(), "replicationFactor", t.getReplicationFactor(),
                        "outOfSyncReplicaCount", 0))
                .toList();
        return Response.ok(Map.of("topics", topics)).build();
    }

    @GET
    @Path("/v1/clusters/{clusterArn}/topics/{topicName}")
    public Response describeTopic(@PathParam("clusterArn") String clusterArn,
                                  @PathParam("topicName") String topicName) {
        return Response.ok(topicView(mskService.describeTopic(clusterArn, topicName))).build();
    }

    @PUT
    @Path("/v1/clusters/{clusterArn}/topics/{topicName}")
    public Response updateTopic(@PathParam("clusterArn") String clusterArn, @PathParam("topicName") String topicName,
                                Map<String, Object> request) {
        MskTopic topic = mskService.updateTopic(clusterArn, topicName,
                asInteger(request.get("partitionCount"), "partitionCount"), asString(request.get("configs"), "configs"));
        return Response.ok(Map.of("topicArn", topic.getTopicArn(), "topicName", topic.getTopicName(),
                "status", "ACTIVE")).build();
    }

    @DELETE
    @Path("/v1/clusters/{clusterArn}/topics/{topicName}")
    public Response deleteTopic(@PathParam("clusterArn") String clusterArn, @PathParam("topicName") String topicName) {
        MskTopic topic = mskService.deleteTopic(clusterArn, topicName);
        return Response.ok(Map.of("topicArn", topic.getTopicArn(), "topicName", topic.getTopicName(),
                "status", "DELETING")).build();
    }

    private Map<String, Object> vpcConnectionView(MskVpcConnection connection) {
        Map<String, Object> view = new HashMap<>();
        view.put("vpcConnectionArn", connection.getVpcConnectionArn());
        view.put("targetClusterArn", connection.getTargetClusterArn());
        view.put("creationTime", connection.getCreationTime().toString());
        view.put("authentication", connection.getAuthentication());
        view.put("vpcId", connection.getVpcId());
        view.put("state", connection.getState());
        return view;
    }

    private Map<String, Object> topicView(MskTopic topic) {
        Map<String, Object> view = new HashMap<>();
        view.put("topicArn", topic.getTopicArn());
        view.put("topicName", topic.getTopicName());
        view.put("partitionCount", topic.getPartitionCount());
        view.put("replicationFactor", topic.getReplicationFactor());
        view.put("configs", topic.getConfigs() != null ? topic.getConfigs()
                : Base64.getEncoder().encodeToString("{}".getBytes(StandardCharsets.UTF_8)));
        view.put("status", "ACTIVE");
        return view;
    }

    private Integer asInteger(Object value, String fieldName) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof Number number)) {
            throw new AwsException("BadRequestException", fieldName + " must be an integer.", 400);
        }
        return number.intValue();
    }

    @POST
    @Path("/v1/clusters/{clusterArn}/scram-secrets")
    public Response batchAssociateScramSecret(@PathParam("clusterArn") String clusterArn,
                                              Map<String, Object> request) {
        List<String> secretArns = request == null ? null
                : asStringList(request.get("secretArnList"), "secretArnList");
        List<Map<String, String>> unprocessedScramSecrets =
                mskScramSecretsService.batchAssociateScramSecret(clusterArn, secretArns);
        return Response.ok(Map.of("clusterArn", clusterArn, "unprocessedScramSecrets", unprocessedScramSecrets)).build();
    }

    @GET
    @Path("/v1/clusters/{clusterArn}/scram-secrets")
    public Response listScramSecrets(@PathParam("clusterArn") String clusterArn,
                                     @QueryParam("maxResults") String maxResults,
                                     @QueryParam("nextToken") String nextToken) {
        PaginatedResult<String> result = mskScramSecretsService.listScramSecrets(clusterArn,
                Pagination.parseMaxResults(maxResults, "BadRequestException"), nextToken);
        Map<String, Object> response = new HashMap<>();
        response.put("secretArnList", result.items());
        if (result.nextToken() != null) {
            response.put("nextToken", result.nextToken());
        }
        return Response.ok(response).build();
    }

    @PATCH
    @Path("/v1/clusters/{clusterArn}/scram-secrets")
    public Response batchDisassociateScramSecret(@PathParam("clusterArn") String clusterArn,
                                                 Map<String, Object> request) {
        List<String> secretArns = request == null ? null
                : asStringList(request.get("secretArnList"), "secretArnList");
        List<Map<String, String>> unprocessedScramSecrets =
                mskScramSecretsService.batchDisassociateScramSecret(clusterArn, secretArns);
        return Response.ok(Map.of("clusterArn", clusterArn, "unprocessedScramSecrets", unprocessedScramSecrets)).build();
    }

    // ── Configurations ───────────────────────────────────────────────────────

    @POST
    @Path("/v1/configurations")
    public Response createConfiguration(Map<String, Object> request) {
        String name = asString(request.get("name"), "name");
        String description = asString(request.get("description"), "description");
        List<String> kafkaVersions = asStringList(request.get("kafkaVersions"), "kafkaVersions");
        String serverProperties = decodeServerProperties(asString(request.get("serverProperties"), "serverProperties"));

        MskConfiguration configuration = mskService.createConfiguration(name, description, kafkaVersions, serverProperties);
        return Response.ok(Map.of(
                "arn", configuration.getArn(),
                "name", configuration.getName(),
                "state", configuration.getState(),
                "creationTime", configuration.getCreationTime(),
                "latestRevision", configuration.getLatestRevision())).build();
    }

    @GET
    @Path("/v1/configurations")
    public Response listConfigurations(@QueryParam("maxResults") String maxResultsParam,
                                        @QueryParam("nextToken") String nextToken) {
        PaginatedResult<MskConfiguration> result = mskService.listConfigurations(
                Pagination.parseMaxResults(maxResultsParam, "BadRequestException"), nextToken);
        List<Map<String, Object>> configurations = result.items().stream()
                .map(this::toConfigurationView)
                .toList();

        Map<String, Object> response = new HashMap<>();
        response.put("configurations", configurations);
        if (result.nextToken() != null) {
            response.put("nextToken", result.nextToken());
        }
        return Response.ok(response).build();
    }

    @GET
    @Path("/v1/configurations/{arn}")
    public Response describeConfiguration(@PathParam("arn") String arn) {
        MskConfiguration configuration = mskService.describeConfiguration(arn);
        return Response.ok(toConfigurationView(configuration)).build();
    }

    @DELETE
    @Path("/v1/configurations/{arn}")
    public Response deleteConfiguration(@PathParam("arn") String arn) {
        mskService.deleteConfiguration(arn);
        return Response.ok(Map.of("arn", arn, "state", "DELETING")).build();
    }

    @PUT
    @Path("/v1/configurations/{arn}")
    public Response updateConfiguration(@PathParam("arn") String arn, Map<String, Object> request) {
        String description = asString(request.get("description"), "description");
        String serverProperties = decodeServerProperties(asString(request.get("serverProperties"), "serverProperties"));

        MskConfiguration configuration = mskService.updateConfiguration(arn, description, serverProperties);
        return Response.ok(Map.of(
                "arn", configuration.getArn(),
                "latestRevision", configuration.getLatestRevision())).build();
    }

    @GET
    @Path("/v1/configurations/{arn}/revisions")
    public Response listConfigurationRevisions(@PathParam("arn") String arn,
                                                @QueryParam("maxResults") String maxResultsParam,
                                                @QueryParam("nextToken") String nextToken) {
        PaginatedResult<ConfigurationRevision> result = mskService.listConfigurationRevisions(arn,
                Pagination.parseMaxResults(maxResultsParam, "BadRequestException"), nextToken);

        Map<String, Object> response = new HashMap<>();
        response.put("revisions", result.items());
        if (result.nextToken() != null) {
            response.put("nextToken", result.nextToken());
        }
        return Response.ok(response).build();
    }

    @GET
    @Path("/v1/configurations/{arn}/revisions/{revision}")
    public Response describeConfigurationRevision(@PathParam("arn") String arn,
                                                    @PathParam("revision") String revisionParam) {
        long revision = parseRevision(revisionParam);
        ConfigurationRevisionDetail detail = mskService.describeConfigurationRevision(arn, revision);

        Map<String, Object> response = new HashMap<>();
        response.put("arn", detail.getArn());
        response.put("creationTime", detail.getCreationTime());
        response.put("description", detail.getDescription() != null ? detail.getDescription() : "");
        response.put("revision", detail.getRevision());
        response.put("serverProperties",
                Base64.getEncoder().encodeToString(detail.getServerProperties().getBytes(StandardCharsets.UTF_8)));
        return Response.ok(response).build();
    }

    // The v1 ClusterInfo is flat: broker node group, encryption, client auth, monitoring and
    // logging all sit directly on the cluster object.
    //
    // Built explicitly rather than by serializing MskCluster, because that model is also the
    // persisted shape and carries internal bookkeeping (bootstrapBrokers, containerId,
    // accountId, volumeId) that must stay in the store but never reach a client.
    private Map<String, Object> toClusterViewV1(MskCluster cluster) {
        Map<String, Object> view = commonClusterFields(cluster);
        view.putAll(provisionedFields(cluster));
        return view;
    }

    // The v2 ClusterInfo is NOT the v1 shape with extra members: everything provisioned-specific
    // nests under "provisioned", and a "clusterType" discriminator selects between that and the
    // serverless shape. An AWS SDK v2 client - which is what terraform-provider-aws calls -
    // looks for these fields only there, so returning them flat leaves them invisible to it.
    private Map<String, Object> toClusterViewV2(MskCluster cluster) {
        Map<String, Object> view = commonClusterFields(cluster);
        view.put("clusterType", clusterType(cluster));
        if (mskService.isServerless(cluster)) {
            putIfPresent(view, "serverless", cluster.getServerless());
        } else {
            view.put("provisioned", provisionedFields(cluster));
        }
        return view;
    }

    private String clusterType(MskCluster cluster) {
        return cluster.getClusterType() != null ? cluster.getClusterType() : PROVISIONED_CLUSTER_TYPE;
    }

    // Members that stay top-level in both versions.
    private Map<String, Object> commonClusterFields(MskCluster cluster) {
        Map<String, Object> view = new HashMap<>();
        view.put("clusterArn", cluster.getClusterArn());
        view.put("clusterName", cluster.getClusterName());
        view.put("state", cluster.getState());
        view.put("creationTime", cluster.getCreationTime());
        view.put("currentVersion", cluster.getCurrentVersion());
        view.put("tags", cluster.getTags() != null ? cluster.getTags() : Map.of());
        return view;
    }

    // Members that are flat on v1's ClusterInfo and nested under v2's "provisioned".
    private Map<String, Object> provisionedFields(MskCluster cluster) {
        Map<String, Object> view = new HashMap<>();
        view.put("numberOfBrokerNodes", cluster.getNumberOfBrokerNodes());
        view.put("zookeeperConnectString", cluster.getZookeeperConnectString());
        view.put("currentBrokerSoftwareInfo", cluster.getCurrentBrokerSoftwareInfo());
        // Absent optional members are omitted rather than sent as null, matching AWS.
        putIfPresent(view, "brokerNodeGroupInfo", cluster.getBrokerNodeGroupInfo());
        putIfPresent(view, "encryptionInfo", cluster.getEncryptionInfo());
        putIfPresent(view, "clientAuthentication", cluster.getClientAuthentication());
        putIfPresent(view, "enhancedMonitoring", cluster.getEnhancedMonitoring());
        putIfPresent(view, "loggingInfo", cluster.getLoggingInfo());
        putIfPresent(view, "openMonitoring", cluster.getOpenMonitoring());
        putIfPresent(view, "storageMode", cluster.getStorageMode());
        putIfPresent(view, "rebalancing", cluster.getRebalancing());
        return view;
    }

    private void putIfPresent(Map<String, Object> view, String key, Object value) {
        if (value != null) {
            view.put(key, value);
        }
    }

    // AWS's Configuration/DescribeConfigurationResponse shape never includes
    // serverProperties (that's only returned via DescribeConfigurationRevision), so build
    // an explicit view instead of serializing the model directly.
    private Map<String, Object> toConfigurationView(MskConfiguration configuration) {
        Map<String, Object> view = new HashMap<>();
        view.put("arn", configuration.getArn());
        view.put("name", configuration.getName());
        view.put("description", configuration.getDescription() != null ? configuration.getDescription() : "");
        view.put("kafkaVersions", configuration.getKafkaVersions() != null ? configuration.getKafkaVersions() : List.of());
        view.put("state", configuration.getState());
        view.put("creationTime", configuration.getCreationTime());
        view.put("latestRevision", configuration.getLatestRevision());
        return view;
    }

    // Bound as String rather than @PathParam long, matching the same reasoning as
    // maxResults: a failed path-param conversion should return an AWS-shaped 400, not
    // whatever the framework's own default handling produces.
    private long parseRevision(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new AwsException("BadRequestException", "revision must be an integer.", 400);
        }
    }

    private String decodeServerProperties(String serverPropertiesB64) {
        if (serverPropertiesB64 == null) {
            return null;
        }
        try {
            return new String(Base64.getDecoder().decode(serverPropertiesB64), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new AwsException("BadRequestException", "serverProperties must be base64-encoded.", 400);
        }
    }

    private String asString(Object value, String fieldName) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof String s)) {
            throw new AwsException("BadRequestException", fieldName + " must be a string.", 400);
        }
        return s;
    }

    private List<String> asStringList(Object value, String fieldName) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof List<?> list) || list.stream().anyMatch(item -> !(item instanceof String))) {
            throw new AwsException("BadRequestException", fieldName + " must be an array of strings.", 400);
        }
        return list.stream().map(String.class::cast).toList();
    }
}
