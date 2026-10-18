package io.github.hectorvent.floci.services.msk;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsArnUtils;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.PaginatedResult;
import io.github.hectorvent.floci.core.common.Pagination;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.common.docker.ContainerStorageHelper;
import io.github.hectorvent.floci.core.resource.ExplorerResource;
import io.github.hectorvent.floci.core.resource.ResourceProvider;
import io.github.hectorvent.floci.core.resource.SupportedResourceType;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.StorageBackend;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.services.msk.model.BrokerNodeGroupInfo;
import io.github.hectorvent.floci.services.msk.model.ClusterState;
import io.github.hectorvent.floci.services.msk.model.ConfigurationInfo;
import io.github.hectorvent.floci.services.msk.model.ConfigurationRevision;
import io.github.hectorvent.floci.services.msk.model.ConfigurationRevisionDetail;
import io.github.hectorvent.floci.services.msk.model.ConfigurationState;
import io.github.hectorvent.floci.services.msk.model.ConnectivityInfo;
import io.github.hectorvent.floci.services.msk.model.CreateClusterRequest;
import io.github.hectorvent.floci.services.msk.model.CreateClusterV2Request;
import io.github.hectorvent.floci.services.msk.model.EncryptionInTransit;
import io.github.hectorvent.floci.services.msk.model.EncryptionInfo;
import io.github.hectorvent.floci.services.msk.model.MskCluster;
import io.github.hectorvent.floci.services.msk.model.MskConfiguration;
import io.github.hectorvent.floci.services.msk.model.MskTopic;
import io.github.hectorvent.floci.services.msk.model.MskVpcConnection;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class MskService implements ResourceProvider {

    private static final Logger LOG = Logger.getLogger(MskService.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String DEFAULT_KAFKA_VERSION = "3.6.0";
    private static final String DEFAULT_ENHANCED_MONITORING = "DEFAULT";
    private static final String DEFAULT_CLIENT_BROKER_ENCRYPTION = "TLS_PLAINTEXT";
    private static final String PROVISIONED_CLUSTER_TYPE = "PROVISIONED";
    private static final String SERVERLESS_CLUSTER_TYPE = "SERVERLESS";
    private static final int MAX_PAGE = 100;
    private static final int MAX_CLUSTER_NAME_LENGTH = 64;
    // Floor only, deliberately no ceiling. The SDK/CLI reference carries an __integerMin1Max15
    // shape for numberOfBrokerNodes, but the REST API reference documents no minimum or maximum
    // for it - on a page that does give ranges for its neighbours (volumeSize 1-16384,
    // revision min 1) - and the quota page allows 30 brokers per ZooKeeper cluster and 60 per
    // KRaft cluster, both adjustable upward. Capping at 15 would reject clusters real MSK
    // accepts and turn a converging-on-the-wrong-number bug into an outright apply failure for
    // every count from 16 up. An emulator that accepts a little more than AWS costs nobody
    // anything; one that rejects a valid request blocks real work.
    private static final int MIN_BROKER_NODES = 1;
    private static final int MIN_EBS_VOLUME_SIZE = 1;
    private static final int MAX_EBS_VOLUME_SIZE = 16384;
    private static final Set<String> ENHANCED_MONITORING_VALUES =
            Set.of("DEFAULT", "PER_BROKER", "PER_TOPIC_PER_BROKER", "PER_TOPIC_PER_PARTITION");
    private static final Set<String> STORAGE_MODE_VALUES = Set.of("LOCAL", "TIERED");
    private static final Set<String> CLIENT_BROKER_VALUES = Set.of("TLS", "TLS_PLAINTEXT", "PLAINTEXT");
    private static final Set<String> REBALANCING_STATUS_VALUES = Set.of("PAUSED", "ACTIVE");
    private final StorageBackend<String, MskCluster> storage;
    private final StorageBackend<String, MskConfiguration> configurationStorage;
    private final StorageBackend<String, MskVpcConnection> vpcConnectionStorage;
    private final StorageBackend<String, MskTopic> topicStorage;
    private final EmulatorConfig config;
    private final RegionResolver regionResolver;
    private final RedpandaManager redpandaManager;
    private final MskBrokerFrontEnd frontEnd;
    // Operations complete as they are requested, so they are kept only for DescribeClusterOperation.
    private final Map<String, Map<String, Object>> operations = new ConcurrentHashMap<>();
    private final ScheduledExecutorService poller = Executors.newSingleThreadScheduledExecutor();
    private final Object configurationUpdateLock = new Object();

    public MskService(StorageFactory storageFactory, EmulatorConfig config,
                      RegionResolver regionResolver, RedpandaManager redpandaManager) {
        this(storageFactory, config, regionResolver, redpandaManager, null);
    }

    @Inject
    public MskService(StorageFactory storageFactory, EmulatorConfig config,
                      RegionResolver regionResolver, RedpandaManager redpandaManager, MskBrokerFrontEnd frontEnd) {
        this.storage = storageFactory.create("msk", "msk-clusters.json", new TypeReference<Map<String, MskCluster>>() {});
        this.configurationStorage = storageFactory.create("msk", "msk-configurations.json",
                new TypeReference<Map<String, MskConfiguration>>() {});
        this.vpcConnectionStorage = storageFactory.create("msk", "msk-vpc-connections.json",
                new TypeReference<Map<String, MskVpcConnection>>() {});
        this.topicStorage = storageFactory.create("msk", "msk-topics.json",
                new TypeReference<Map<String, MskTopic>>() {});
        this.config = config;
        this.regionResolver = regionResolver;
        this.redpandaManager = redpandaManager;
        this.frontEnd = frontEnd;
    }

    @PostConstruct
    public void init() {
        startReadinessPoller();
    }

    @PreDestroy
    public void shutdown() {
        poller.shutdown();
        if (!config.services().msk().mock()) {
            for (MskCluster cluster : allClusters()) {
                redpandaManager.stopContainer(cluster);
            }
        }
    }

    public MskCluster createCluster(String clusterName) {
        return createCluster(clusterName, DEFAULT_KAFKA_VERSION);
    }

    public MskCluster createCluster(String clusterName, String kafkaVersion) {
        CreateClusterRequest request = new CreateClusterRequest();
        request.setClusterName(clusterName);
        request.setKafkaVersion(kafkaVersion);
        return createCluster(request);
    }

    public MskCluster createCluster(CreateClusterRequest request) {
        validateCreateRequest(request);
        String clusterName = request.getClusterName();
        if (storage.scan(k -> true).stream().anyMatch(c -> isCurrentRegion(c)
                && c.getClusterName().equals(clusterName))) {
            throw new AwsException("ConflictException", "Cluster already exists: " + clusterName, 409);
        }

        String accountId = regionResolver.getAccountId();
        String clusterArn = AwsArnUtils.Arn.of("kafka", regionResolver.getRegion(), accountId,
                "cluster/" + clusterName + "/" + UUID.randomUUID()).toString();

        String kafkaVersion = request.getKafkaVersion();
        String resolvedKafkaVersion = (kafkaVersion == null || kafkaVersion.isBlank()) ? DEFAULT_KAFKA_VERSION : kafkaVersion;
        MskCluster cluster = new MskCluster(clusterArn, clusterName, resolvedKafkaVersion);
        cluster.setClusterType(PROVISIONED_CLUSTER_TYPE);
        cluster.setAccountId(accountId);
        cluster.setResourceRegion(regionResolver.getRegion());
        cluster.setVolumeId(String.format("%06x", new SecureRandom().nextInt(0xFFFFFF)));
        // Stamp the volume name now, with the current prefix, so it is persisted rather than
        // recomputed later. Only records predating this field fall back to the legacy name.
        cluster.setDockerVolumeName(ContainerStorageHelper.resourceName(
                config, "msk", cluster.getVolumeId(), cluster.getClusterName()));

        if (request.getNumberOfBrokerNodes() != null) {
            cluster.setNumberOfBrokerNodes(request.getNumberOfBrokerNodes());
        }
        cluster.setTags(request.getTags());
        cluster.setBrokerNodeGroupInfo(request.getBrokerNodeGroupInfo());
        cluster.setClientAuthentication(request.getClientAuthentication());
        cluster.setLoggingInfo(request.getLoggingInfo());
        cluster.setOpenMonitoring(request.getOpenMonitoring());
        cluster.setStorageMode(request.getStorageMode());
        cluster.setRebalancing(request.getRebalancing());

        // AWS applies these server-side when the member is absent, and echoes the resolved
        // value back on Describe. Leaving them unset instead is what keeps a terraform plan
        // permanently dirty: the provider's schema defaults them, so a null in the response
        // reads as a drift against config the user never wrote.
        cluster.setEnhancedMonitoring(request.getEnhancedMonitoring() != null
                ? request.getEnhancedMonitoring() : DEFAULT_ENHANCED_MONITORING);
        cluster.setEncryptionInfo(withEncryptionDefaults(request.getEncryptionInfo()));

        // configurationInfo is a request-only member: the response carries the configuration
        // on currentBrokerSoftwareInfo instead (see BrokerSoftwareInfo).
        ConfigurationInfo configurationInfo = request.getConfigurationInfo();
        if (configurationInfo != null) {
            cluster.getCurrentBrokerSoftwareInfo().setConfigurationArn(configurationInfo.getArn());
            cluster.getCurrentBrokerSoftwareInfo().setConfigurationRevision(configurationInfo.getRevision());
        }

        startBroker(cluster);

        storage.put(clusterArn, cluster);
        return cluster;
    }

    public MskCluster createCluster(CreateClusterV2Request request) {
        // A CreateClusterV2 request carries exactly one of provisioned/serverless. A serverless
        // request used to be flattened into a provisioned cluster, which then reported itself as
        // provisioned and echoed broker metadata the caller never asked for.
        if (request.getServerless() != null) {
            if (request.getProvisioned() != null) {
                throw badRequest("serverless",
                        "Exactly one of provisioned and serverless must be specified.");
            }
            return createServerlessCluster(request);
        }

        CreateClusterRequest merged = new CreateClusterRequest();
        merged.setClusterName(request.getClusterName());
        merged.setTags(request.getTags());
        if (request.getProvisioned() != null) {
            merged.setKafkaVersion(request.getProvisioned().getKafkaVersion());
            // A malformed numberOfBrokerNodes carries no int value to copy (see BrokerCount),
            // so its "malformed" state has to be propagated explicitly rather than lost when
            // getNumberOfBrokerNodes() returns null the same way "absent" would.
            if (request.getProvisioned().isNumberOfBrokerNodesMalformed()) {
                merged.markNumberOfBrokerNodesMalformed();
            } else {
                merged.setNumberOfBrokerNodes(request.getProvisioned().getNumberOfBrokerNodes());
            }
            merged.setBrokerNodeGroupInfo(request.getProvisioned().getBrokerNodeGroupInfo());
            merged.setEncryptionInfo(request.getProvisioned().getEncryptionInfo());
            merged.setClientAuthentication(request.getProvisioned().getClientAuthentication());
            merged.setEnhancedMonitoring(request.getProvisioned().getEnhancedMonitoring());
            merged.setLoggingInfo(request.getProvisioned().getLoggingInfo());
            merged.setConfigurationInfo(request.getProvisioned().getConfigurationInfo());
            merged.setOpenMonitoring(request.getProvisioned().getOpenMonitoring());
            merged.setStorageMode(request.getProvisioned().getStorageMode());
            merged.setRebalancing(request.getProvisioned().getRebalancing());
        }
        return createCluster(merged);
    }

    private void startBroker(MskCluster cluster) {
        if (config.services().msk().mock()) {
            cluster.setState(ClusterState.ACTIVE);
            cluster.setBootstrapBrokers("localhost:9092");
        } else if (!redpandaManager.tryStartContainer(cluster)) {
            LOG.warnv("MSK cluster {0} created without a backing Kafka broker: no Docker daemon "
                    + "is reachable. Metadata operations work; producing and consuming do not "
                    + "until a daemon appears.", cluster.getClusterName());
            cluster.setState(ClusterState.ACTIVE);
            cluster.setBootstrapBrokers("localhost:9092");
        }
    }

    /**
     * Rejects the CreateCluster input AWS rejects.
     *
     * <p>Scoped to the members AWS documents an actual constraint for, plus {@code clusterName},
     * whose absence used to build an ARN containing {@code cluster/null/<uuid>} and store a
     * cluster that could never be addressed by name.
     *
     * <p>Deliberately NOT enforced: the *presence* of {@code kafkaVersion},
     * {@code numberOfBrokerNodes} and {@code brokerNodeGroupInfo}, which real MSK requires.
     * The emulator has always defaulted those, and tightening it would reject minimal fixtures
     * across the suite for no emulation benefit - a separate, deliberate break rather than
     * something to slip into a metadata round-trip fix.
     */
    private void validateCreateRequest(CreateClusterRequest request) {
        String clusterName = request.getClusterName();
        if (clusterName == null || clusterName.isBlank()) {
            throw badRequest("clusterName", "clusterName is required.");
        }
        if (clusterName.length() > MAX_CLUSTER_NAME_LENGTH) {
            throw badRequest("clusterName",
                    "clusterName must be between 1 and " + MAX_CLUSTER_NAME_LENGTH + " characters.");
        }

        // A fractional or otherwise non-integral numberOfBrokerNodes (e.g. 2.7, or a literal
        // like 1.0000000000000001 that only looks whole once collapsed into a double) is
        // malformed rather than something to round - see BrokerCountDeserializer/BrokerCount.
        if (request.isNumberOfBrokerNodesMalformed()) {
            throw badRequest("numberOfBrokerNodes", "numberOfBrokerNodes must be a whole number.");
        }

        Integer brokerNodes = request.getNumberOfBrokerNodes();
        if (brokerNodes != null && brokerNodes < MIN_BROKER_NODES) {
            throw badRequest("numberOfBrokerNodes",
                    "numberOfBrokerNodes must be at least " + MIN_BROKER_NODES + ".");
        }

        BrokerNodeGroupInfo nodeGroup = request.getBrokerNodeGroupInfo();
        if (nodeGroup != null && nodeGroup.getStorageInfo() != null
                && nodeGroup.getStorageInfo().getEbsStorageInfo() != null) {
            Integer volumeSize = nodeGroup.getStorageInfo().getEbsStorageInfo().getVolumeSize();
            if (volumeSize != null && (volumeSize < MIN_EBS_VOLUME_SIZE || volumeSize > MAX_EBS_VOLUME_SIZE)) {
                throw badRequest("volumeSize",
                        "volumeSize must be between " + MIN_EBS_VOLUME_SIZE + " and " + MAX_EBS_VOLUME_SIZE + ".");
            }
        }

        ConfigurationInfo configurationInfo = request.getConfigurationInfo();
        if (configurationInfo != null && configurationInfo.getRevision() != null
                && configurationInfo.getRevision() < 1) {
            throw badRequest("configurationInfo.revision", "configurationInfo.revision must be at least 1.");
        }

        validateEnum("enhancedMonitoring", request.getEnhancedMonitoring(), ENHANCED_MONITORING_VALUES);
        validateEnum("storageMode", request.getStorageMode(), STORAGE_MODE_VALUES);
        if (request.getEncryptionInfo() != null && request.getEncryptionInfo().getEncryptionInTransit() != null) {
            validateEnum("encryptionInfo.encryptionInTransit.clientBroker",
                    request.getEncryptionInfo().getEncryptionInTransit().getClientBroker(), CLIENT_BROKER_VALUES);
        }
        if (request.getRebalancing() != null) {
            validateEnum("rebalancing.status", request.getRebalancing().getStatus(), REBALANCING_STATUS_VALUES);
        }
    }

    private void validateEnum(String field, String value, Set<String> allowed) {
        if (value != null && !allowed.contains(value)) {
            throw badRequest(field,
                    field + " must be one of " + String.join(", ", new TreeSet<>(allowed)) + ".");
        }
    }

    // MSK's Error schema is two members - message and invalidParameter, "the parameter that
    // caused the error" - so a validation failure names the member it rejected.
    private AwsException badRequest(String invalidParameter, String message) {
        return new AwsException("BadRequestException", message, 400,
                Map.of("invalidParameter", invalidParameter));
    }

    /**
     * Fills in the encryption-in-transit defaults AWS applies when CreateCluster omits them:
     * {@code clientBroker} defaults to TLS_PLAINTEXT and {@code inCluster} to true. An
     * explicitly supplied value is never overwritten. {@code encryptionAtRest} is left alone -
     * real MSK creates a KMS key for you there, which the emulator has no equivalent of, so
     * inventing an ARN would be worse than omitting the member.
     */
    private EncryptionInfo withEncryptionDefaults(EncryptionInfo requested) {
        EncryptionInfo encryptionInfo = requested != null ? requested : new EncryptionInfo();
        EncryptionInTransit inTransit = encryptionInfo.getEncryptionInTransit();
        if (inTransit == null) {
            inTransit = new EncryptionInTransit();
            encryptionInfo.setEncryptionInTransit(inTransit);
        }
        if (inTransit.getClientBroker() == null) {
            inTransit.setClientBroker(DEFAULT_CLIENT_BROKER_ENCRYPTION);
        }
        if (inTransit.getInCluster() == null) {
            inTransit.setInCluster(true);
        }
        return encryptionInfo;
    }

    /**
     * Serverless clusters have no broker node group, instance type or broker count to speak of -
     * AWS manages that - so none of the provisioned metadata is stored or echoed for them. The
     * emulated Kafka endpoint behind the cluster is the same either way.
     */
    private MskCluster createServerlessCluster(CreateClusterV2Request request) {
        String clusterName = request.getClusterName();
        if (clusterName == null || clusterName.isBlank()) {
            throw badRequest("clusterName", "clusterName is required.");
        }
        if (clusterName.length() > MAX_CLUSTER_NAME_LENGTH) {
            throw badRequest("clusterName",
                    "clusterName must be between 1 and " + MAX_CLUSTER_NAME_LENGTH + " characters.");
        }
        if (storage.scan(k -> true).stream().anyMatch(c -> isCurrentRegion(c)
                && c.getClusterName().equals(clusterName))) {
            throw new AwsException("ConflictException", "Cluster already exists: " + clusterName, 409);
        }

        String accountId = regionResolver.getAccountId();
        String clusterArn = AwsArnUtils.Arn.of("kafka", regionResolver.getRegion(), accountId,
                "cluster/" + clusterName + "/" + UUID.randomUUID()).toString();

        MskCluster cluster = new MskCluster(clusterArn, clusterName, DEFAULT_KAFKA_VERSION);
        cluster.setClusterType(SERVERLESS_CLUSTER_TYPE);
        cluster.setServerless(request.getServerless());
        cluster.setTags(request.getTags());
        cluster.setAccountId(accountId);
        cluster.setResourceRegion(regionResolver.getRegion());
        cluster.setVolumeId(String.format("%06x", new SecureRandom().nextInt(0xFFFFFF)));
        // Stamp the volume name now, with the current prefix, so it is persisted rather than
        // recomputed later. Only records predating this field fall back to the legacy name.
        cluster.setDockerVolumeName(ContainerStorageHelper.resourceName(
                config, "msk", cluster.getVolumeId(), cluster.getClusterName()));

        // Provisioned-only members must not surface on a serverless cluster.
        cluster.setNumberOfBrokerNodes(0);
        cluster.setZookeeperConnectString(null);
        cluster.setCurrentBrokerSoftwareInfo(null);

        startBroker(cluster);

        storage.put(clusterArn, cluster);
        return cluster;
    }

    public boolean isServerless(MskCluster cluster) {
        return SERVERLESS_CLUSTER_TYPE.equals(cluster.getClusterType());
    }

    /**
     * DescribeCluster for the v1 API, which predates serverless: its ClusterInfo has no way to
     * represent one, and real MSK answers a v1 describe of a serverless cluster with a
     * BadRequestException pointing the caller at DescribeClusterV2.
     */
    public MskCluster describeClusterV1(String clusterArn) {
        MskCluster cluster = describeCluster(clusterArn);
        if (isServerless(cluster)) {
            throw new AwsException("BadRequestException",
                    "This operation cannot be performed on serverless clusters. Use DescribeClusterV2 instead.", 400);
        }
        return cluster;
    }

    /** ListClusters for the v1 API, which likewise cannot represent serverless clusters. */
    public List<MskCluster> listProvisionedClusters() {
        return storage.scan(k -> true).stream().filter(this::isCurrentRegion)
                .filter(c -> !isServerless(c)).toList();
    }

    public MskCluster describeCluster(String clusterArn) {
        return storage.get(clusterArn).filter(this::isCurrentRegion)
                .orElseThrow(() -> new AwsException("NotFoundException", "Cluster not found: " + clusterArn, 404));
    }

    public List<MskCluster> listClusters() {
        return storage.scan(k -> true).stream().filter(this::isCurrentRegion).toList();
    }

    public void deleteCluster(String clusterArn) {
        MskCluster cluster = describeCluster(clusterArn);

        cluster.setState(ClusterState.DELETING);
        if (!config.services().msk().mock()) {
            redpandaManager.stopContainer(cluster);
            redpandaManager.removeClusterStorage(cluster);
        }
        storage.delete(clusterArn);
    }

    public String getBootstrapBrokers(String clusterArn) {
        MskCluster cluster = describeCluster(clusterArn);
        return cluster.getBootstrapBrokers();
    }

    /** Every bootstrap string the cluster's listeners give, keyed by the response member name. */
    public Map<String, String> bootstrapBrokerStrings(String clusterArn) {
        MskCluster cluster = clusterInAnyAccount(clusterArn);
        Map<String, String> strings = new LinkedHashMap<>();
        if (cluster.getBootstrapBrokers() != null) {
            strings.put("bootstrapBrokerString", cluster.getBootstrapBrokers());
        }
        putIfPresent(strings, "bootstrapBrokerStringTls",
                MskBrokerEndpoints.bootstrapString(cluster, MskBrokerEndpoints.Family.TLS));
        putIfPresent(strings, "bootstrapBrokerStringSaslIam",
                MskBrokerEndpoints.bootstrapString(cluster, MskBrokerEndpoints.Family.SASL_IAM));
        putIfPresent(strings, "bootstrapBrokerStringVpcConnectivitySaslIam",
                MskBrokerEndpoints.bootstrapString(cluster, MskBrokerEndpoints.Family.VPC_CONNECTIVITY_SASL_IAM));
        return strings;
    }

    private static void putIfPresent(Map<String, String> map, String key, String value) {
        if (value != null) {
            map.put(key, value);
        }
    }

    /** By ARN in the ARN's own account, as a cluster shared through its cluster policy is reached. */
    public MskCluster clusterInAnyAccount(String clusterArn) {
        if (storage instanceof AccountAwareStorageBackend<MskCluster> aware && AwsArnUtils.isArn(clusterArn)) {
            String account = AwsArnUtils.parse(clusterArn).accountId();
            Optional<MskCluster> cluster = aware.getForAccount(account, clusterArn);
            if (cluster.isPresent()) {
                return cluster.get();
            }
        }
        return describeCluster(clusterArn);
    }

    /** Applies the connectivity at once; the returned operation already reports UPDATE_COMPLETE. */
    public String updateConnectivity(String clusterArn, ConnectivityInfo connectivity, String currentVersion) {
        MskCluster cluster = describeCluster(clusterArn);
        if (currentVersion == null) {
            throw badRequest("currentVersion", "currentVersion is required.");
        }
        if (!currentVersion.equals(cluster.getCurrentVersion())) {
            throw new AwsException("BadRequestException",
                    "The specified currentVersion does not match the cluster's current version.", 400);
        }
        BrokerNodeGroupInfo nodes = cluster.getBrokerNodeGroupInfo() != null
                ? cluster.getBrokerNodeGroupInfo() : new BrokerNodeGroupInfo();
        ConnectivityInfo current = nodes.getConnectivityInfo() != null ? nodes.getConnectivityInfo() : new ConnectivityInfo();
        if (connectivity != null) {
            if (connectivity.getVpcConnectivity() != null) {
                current.setVpcConnectivity(connectivity.getVpcConnectivity());
            }
            if (connectivity.getPublicAccess() != null) {
                current.setPublicAccess(connectivity.getPublicAccess());
            }
            if (connectivity.getNetworkType() != null) {
                current.setNetworkType(connectivity.getNetworkType());
            }
        }
        nodes.setConnectivityInfo(current);
        cluster.setBrokerNodeGroupInfo(nodes);
        cluster.setCurrentVersion(Long.toString(System.nanoTime(), 36).toUpperCase(Locale.ROOT));
        putCluster(cluster);
        String operationArn = clusterArn.replace(":cluster/", ":cluster-operation/") + "/" + UUID.randomUUID();
        Map<String, Object> operation = new LinkedHashMap<>();
        operation.put("clusterArn", clusterArn);
        operation.put("operationArn", operationArn);
        operation.put("operationType", "UPDATE_CONNECTIVITY");
        operation.put("operationState", "UPDATE_COMPLETE");
        operation.put("creationTime", Instant.now().toString());
        operation.put("endTime", Instant.now().toString());
        operations.put(operationArn, operation);
        return operationArn;
    }

    public Map<String, Object> describeClusterOperation(String operationArn) {
        Map<String, Object> operation = operations.get(operationArn);
        if (operation == null) {
            throw new AwsException("NotFoundException", "Cluster operation not found: " + operationArn, 404);
        }
        return operation;
    }

    public String putClusterPolicy(String clusterArn, String policy, String currentVersion) {
        MskCluster cluster = describeCluster(clusterArn);
        if (policy == null || policy.isBlank()) {
            throw badRequest("policy", "policy is required.");
        }
        if (currentVersion != null && !currentVersion.equals(cluster.getPolicyVersion())) {
            throw new AwsException("BadRequestException", "currentVersion does not match the policy's version.", 400);
        }
        cluster.setPolicy(policy);
        cluster.setPolicyVersion(Long.toString(System.nanoTime(), 36).toUpperCase(Locale.ROOT));
        putCluster(cluster);
        return cluster.getPolicyVersion();
    }

    public MskCluster getClusterPolicy(String clusterArn) {
        MskCluster cluster = describeCluster(clusterArn);
        if (cluster.getPolicy() == null) {
            throw new AwsException("NotFoundException", "No policy is attached to " + clusterArn, 404);
        }
        return cluster;
    }

    public void deleteClusterPolicy(String clusterArn) {
        MskCluster cluster = describeCluster(clusterArn);
        cluster.setPolicy(null);
        cluster.setPolicyVersion(null);
        putCluster(cluster);
    }

    public MskVpcConnection createVpcConnection(String targetClusterArn, String authentication, String vpcId,
                                                List<String> clientSubnets, List<String> securityGroups,
                                                Map<String, String> tags) {
        if (targetClusterArn == null || authentication == null || vpcId == null) {
            throw badRequest("targetClusterArn", "targetClusterArn, authentication and vpcId are required.");
        }
        MskCluster cluster = clusterInAnyAccount(targetClusterArn);
        MskVpcConnection connection = new MskVpcConnection();
        String accountId = regionResolver.getAccountId();
        connection.setVpcConnectionArn(AwsArnUtils.Arn.of("kafka", regionResolver.getRegion(), accountId,
                "vpc-connection/" + accountId + "/" + cluster.getClusterName() + "/" + UUID.randomUUID()).toString());
        connection.setTargetClusterArn(targetClusterArn);
        connection.setAuthentication(authentication);
        connection.setVpcId(vpcId);
        connection.setClientSubnets(clientSubnets != null ? clientSubnets : List.of());
        connection.setSecurityGroups(securityGroups != null ? securityGroups : List.of());
        connection.setCreationTime(Instant.now());
        connection.setState("AVAILABLE");
        connection.setAccountId(accountId);
        connection.setRegion(regionResolver.getRegion());
        connection.setTags(tags != null ? new HashMap<>(tags) : new HashMap<>());
        vpcConnectionStorage.put(connection.getVpcConnectionArn(), connection);
        return connection;
    }

    public MskVpcConnection describeVpcConnection(String arn) {
        return vpcConnectionStorage.get(arn)
                .orElseThrow(() -> new AwsException("NotFoundException", "VPC connection not found: " + arn, 404));
    }

    public List<MskVpcConnection> listVpcConnections() {
        return vpcConnectionStorage.scan(k -> true).stream()
                .filter(c -> regionResolver.getRegion().equals(c.getRegion())).toList();
    }

    /** The connections other accounts made to a cluster this account owns, whichever account holds them. */
    public List<MskVpcConnection> listClientVpcConnections(String clusterArn) {
        describeCluster(clusterArn);
        List<MskVpcConnection> all = vpcConnectionStorage instanceof AccountAwareStorageBackend<MskVpcConnection> aware
                ? aware.scanAllAccounts() : vpcConnectionStorage.scan(k -> true);
        return all.stream().filter(c -> clusterArn.equals(c.getTargetClusterArn())).toList();
    }

    public MskVpcConnection deleteVpcConnection(String arn) {
        MskVpcConnection connection = describeVpcConnection(arn);
        vpcConnectionStorage.delete(arn);
        connection.setState("DELETING");
        return connection;
    }

    public MskTopic createTopic(String clusterArn, String topicName, Integer partitionCount,
                                Integer replicationFactor, String configs) {
        MskCluster cluster = describeCluster(clusterArn);
        if (topicName == null || topicName.isBlank() || partitionCount == null || partitionCount < 1
                || replicationFactor == null || replicationFactor < 1) {
            throw badRequest("topicName", "topicName, partitionCount and replicationFactor are required.");
        }
        String key = topicKey(clusterArn, topicName);
        if (topicStorage.get(key).isPresent()) {
            throw new AwsException("ConflictException", "Topic already exists: " + topicName, 409);
        }
        MskTopic topic = new MskTopic();
        topic.setTopicArn(clusterArn.replace(":cluster/", ":topic/") + "/" + topicName);
        topic.setClusterArn(clusterArn);
        topic.setTopicName(topicName);
        topic.setPartitionCount(partitionCount);
        topic.setReplicationFactor(replicationFactor);
        topic.setConfigs(configs);
        if (!config.services().msk().mock()) {
            List<String> args = new ArrayList<>(List.of("topic", "create", topicName,
                    "-p", String.valueOf(partitionCount), "-r", "1"));
            topicConfigs(configs).forEach((k, v) -> args.addAll(List.of("-c", k + "=" + v)));
            redpandaManager.rpk(cluster, args);
        }
        topicStorage.put(key, topic);
        return topic;
    }

    public MskTopic describeTopic(String clusterArn, String topicName) {
        describeCluster(clusterArn);
        return topicStorage.get(topicKey(clusterArn, topicName))
                .orElseThrow(() -> new AwsException("NotFoundException", "Topic not found: " + topicName, 404));
    }

    /** @param topicNameFilter a prefix the topic names must start with, or null for every topic */
    public List<MskTopic> listTopics(String clusterArn, String topicNameFilter) {
        describeCluster(clusterArn);
        return topicStorage.scan(k -> true).stream()
                .filter(t -> clusterArn.equals(t.getClusterArn()))
                .filter(t -> topicNameFilter == null || t.getTopicName().startsWith(topicNameFilter))
                .toList();
    }

    public MskTopic updateTopic(String clusterArn, String topicName, Integer partitionCount, String configs) {
        MskTopic topic = describeTopic(clusterArn, topicName);
        MskCluster cluster = describeCluster(clusterArn);
        if (partitionCount != null && partitionCount != topic.getPartitionCount()) {
            if (partitionCount < topic.getPartitionCount()) {
                throw badRequest("partitionCount", "The partition count can only be increased.");
            }
            if (!config.services().msk().mock()) {
                redpandaManager.rpk(cluster, List.of("topic", "add-partitions", topicName,
                        "--num", String.valueOf(partitionCount - topic.getPartitionCount())));
            }
            topic.setPartitionCount(partitionCount);
        }
        if (configs != null) {
            topic.setConfigs(configs);
            if (!config.services().msk().mock() && !topicConfigs(configs).isEmpty()) {
                List<String> args = new ArrayList<>(List.of("topic", "alter-config", topicName));
                topicConfigs(configs).forEach((k, v) -> args.addAll(List.of("--set", k + "=" + v)));
                redpandaManager.rpk(cluster, args);
            }
        }
        topicStorage.put(topicKey(clusterArn, topicName), topic);
        return topic;
    }

    public MskTopic deleteTopic(String clusterArn, String topicName) {
        MskTopic topic = describeTopic(clusterArn, topicName);
        if (!config.services().msk().mock()) {
            redpandaManager.rpk(describeCluster(clusterArn), List.of("topic", "delete", topicName));
        }
        topicStorage.delete(topicKey(clusterArn, topicName));
        return topic;
    }

    private static String topicKey(String clusterArn, String topicName) {
        return clusterArn + "|" + topicName;
    }

    /** The topic API carries configs as base64 of a JSON object of strings. */
    private static Map<String, String> topicConfigs(String configs) {
        Map<String, String> result = new LinkedHashMap<>();
        if (configs == null || configs.isBlank()) {
            return result;
        }
        try {
            String json = new String(Base64.getDecoder().decode(configs), StandardCharsets.UTF_8);
            JSON.readTree(json).fields()
                    .forEachRemaining(e -> result.put(e.getKey(), e.getValue().asText()));
        } catch (Exception e) {
            throw new AwsException("BadRequestException", "configs must be base64-encoded JSON.", 400);
        }
        return result;
    }

    public MskConfiguration createConfiguration(String name, String description,
                                                 List<String> kafkaVersions, String serverProperties) {
        if (name == null || name.isBlank()) {
            throw new AwsException("BadRequestException", "name is required.", 400);
        }
        if (!name.matches("[0-9A-Za-z][0-9A-Za-z-]*")) {
            throw new AwsException("BadRequestException",
                    "Configuration name must match the pattern \"^[0-9A-Za-z][0-9A-Za-z-]{0,}$\".", 400);
        }
        // Only an absent member (null) is rejected. A zero-length blob is a legitimate value
        // meaning "no property overrides": Terraform modules that build serverProperties by
        // joining a map defaulting to {} send exactly that, and real MSK accepts it.
        if (serverProperties == null) {
            throw new AwsException("BadRequestException", "serverProperties is required.", 400);
        }
        if (configurationStorage.scan(k -> true).stream()
                .filter(this::isCurrentRegion)
                .anyMatch(c -> c.getName().equals(name))) {
            throw new AwsException("ConflictException", "Configuration already exists: " + name, 409);
        }

        String accountId = regionResolver.getAccountId();
        String arn = AwsArnUtils.Arn.of("kafka", regionResolver.getRegion(), accountId,
                "configuration/" + name + "/" + UUID.randomUUID()).toString();

        MskConfiguration configuration = new MskConfiguration(arn, name, description, kafkaVersions, serverProperties);
        configuration.setAccountId(accountId);

        configurationStorage.put(arn, configuration);
        LOG.infov("Created MSK configuration: {0}", name);
        return configuration;
    }

    public MskConfiguration describeConfiguration(String arn) {
        // Real MSK reports an unknown configuration ARN as BadRequestException (400), not
        // NotFoundException. The message prefix is a compatibility contract: terraform-provider-aws
        // (and the Pulumi provider bridging it) substring-matches "Configuration ARN does not exist"
        // to detect that a configuration is gone, so its post-delete waiter can complete.
        return configurationStorage.get(arn).filter(this::isCurrentRegion)
                .orElseThrow(() -> new AwsException("BadRequestException",
                        "Configuration ARN does not exist: " + arn, 400));
    }

    public PaginatedResult<MskConfiguration> listConfigurations(Integer maxResults, String nextToken) {
        List<MskConfiguration> all = configurationStorage.scan(k -> true).stream().filter(this::isCurrentRegion).toList();
        return Pagination.paginate(all, MskConfiguration::getArn, maxResults, nextToken, MAX_PAGE, "BadRequestException");
    }

    public MskConfiguration deleteConfiguration(String arn) {
        // Shares updateConfiguration's lock so the two can't interleave: without it, a delete
        // could remove the entry while an in-flight update still holds its pre-delete read,
        // then that update's own put(arn, ...) at the end of its critical section would put the
        // "deleted" configuration right back, resurrecting it.
        synchronized (configurationUpdateLock) {
            MskConfiguration configuration = describeConfiguration(arn);
            configuration.setState(ConfigurationState.DELETING);
            configurationStorage.delete(arn);
            LOG.infov("Deleted MSK configuration: {0}", configuration.getName());
            return configuration;
        }
    }

    public MskConfiguration updateConfiguration(String arn, String description, String serverProperties) {
        // Same absent-vs-empty distinction as createConfiguration: "" is a valid revision
        // body that clears every override, only a missing member is an error.
        if (serverProperties == null) {
            throw new AwsException("BadRequestException", "serverProperties is required.", 400);
        }

        // Read-modify-write on the shared configuration: two concurrent updates for the same
        // ARN could otherwise derive the same "next revision" number before either persists,
        // silently overwriting one request's serverProperties under the other's revision key.
        synchronized (configurationUpdateLock) {
            MskConfiguration configuration = describeConfiguration(arn);
            if (configuration.getState() != ConfigurationState.ACTIVE) {
                throw new AwsException("BadRequestException",
                        "Configuration must be ACTIVE to update: " + arn, 400);
            }
            ConfigurationRevision latestRevision = configuration.getLatestRevision();
            if (latestRevision == null) {
                // MskConfiguration now maps a pre-revision-history entry onto revision 1 as
                // it loads, so this no longer catches every configuration written before the
                // schema changed - only one with no revision data at all to build on (a
                // hand-edited store, or an entry whose latestRevision was explicitly null).
                throw new AwsException("BadRequestException",
                        "Configuration has no revision history and cannot be updated: " + arn, 400);
            }

            long newRevisionNumber = latestRevision.getRevision() + 1;
            configuration.addRevision(new ConfigurationRevision(newRevisionNumber, Instant.now(), description), serverProperties);

            configurationStorage.put(arn, configuration);
            LOG.infov("Updated MSK configuration {0} to revision {1}", configuration.getName(), newRevisionNumber);
            return configuration;
        }
    }

    public PaginatedResult<ConfigurationRevision> listConfigurationRevisions(String arn, Integer maxResults, String nextToken) {
        // Snapshot revisions under the same lock updateConfiguration writes under. Now that
        // MskConfiguration's collections are concurrent-safe this is no longer what prevents a
        // torn read - getRevisions() copies a CopyOnWriteArrayList, which cannot tear on its
        // own. It is kept so every reader of revision state serializes against an in-flight
        // update the same way, rather than leaving describeConfigurationRevision - which reads
        // both collections and does need them to agree - as the only one holding the lock.
        List<ConfigurationRevision> revisions;
        synchronized (configurationUpdateLock) {
            revisions = describeConfiguration(arn).getRevisions();
        }
        return Pagination.paginate(revisions,
                revision -> String.format("%019d", revision.getRevision()),
                maxResults, nextToken, MAX_PAGE, "BadRequestException");
    }

    public ConfigurationRevisionDetail describeConfigurationRevision(String arn, long revision) {
        ConfigurationRevision found;
        String serverProperties;
        synchronized (configurationUpdateLock) {
            MskConfiguration configuration = describeConfiguration(arn);
            found = configuration.getRevisions().stream()
                    .filter(r -> r.getRevision() == revision)
                    .findFirst()
                    .orElseThrow(() -> new AwsException("NotFoundException",
                            "Revision not found: " + revision + " for configuration " + arn, 404));
            serverProperties = configuration.getServerPropertiesByRevision().get(revision);
        }
        return new ConfigurationRevisionDetail(arn, found.getCreationTime(), found.getDescription(),
                revision, serverProperties);
    }

    // ── Tags (ListTagsForResource / TagResource / UntagResource on /v1/tags/{arn}) ──────────
    //
    // MSK tags both clusters and configurations, and the tag path carries only an ARN, so the
    // resource type is resolved by looking the ARN up in each store rather than by parsing it.
    // An unknown ARN is a 404 here, which is what the tags path documents - unlike
    // DescribeConfiguration, whose 400 is a deliberate terraform-provider contract.

    public Map<String, String> listTagsForResource(String arn) {
        MskCluster cluster = storage.get(arn).filter(this::isCurrentRegion).orElse(null);
        if (cluster != null) {
            return cluster.getTags() != null ? cluster.getTags() : Map.of();
        }
        MskConfiguration configuration = configurationStorage.get(arn).filter(this::isCurrentRegion).orElse(null);
        if (configuration != null) {
            return configuration.getTags() != null ? configuration.getTags() : Map.of();
        }
        MskVpcConnection connection = vpcConnectionStorage.get(arn).orElse(null);
        if (connection != null) {
            return connection.getTags() != null ? connection.getTags() : Map.of();
        }
        throw taggedResourceNotFound(arn);
    }

    public void tagResource(String arn, Map<String, String> tags) {
        MskCluster cluster = storage.get(arn).filter(this::isCurrentRegion).orElse(null);
        if (cluster != null) {
            cluster.setTags(merged(cluster.getTags(), tags));
            storage.put(arn, cluster);
            return;
        }
        MskConfiguration configuration = configurationStorage.get(arn).filter(this::isCurrentRegion).orElse(null);
        if (configuration != null) {
            configuration.setTags(merged(configuration.getTags(), tags));
            configurationStorage.put(arn, configuration);
            return;
        }
        MskVpcConnection connection = vpcConnectionStorage.get(arn).orElse(null);
        if (connection != null) {
            connection.setTags(merged(connection.getTags(), tags));
            vpcConnectionStorage.put(arn, connection);
            return;
        }
        throw taggedResourceNotFound(arn);
    }

    public void untagResource(String arn, List<String> tagKeys) {
        MskCluster cluster = storage.get(arn).filter(this::isCurrentRegion).orElse(null);
        if (cluster != null) {
            cluster.setTags(without(cluster.getTags(), tagKeys));
            storage.put(arn, cluster);
            return;
        }
        MskConfiguration configuration = configurationStorage.get(arn).filter(this::isCurrentRegion).orElse(null);
        if (configuration != null) {
            configuration.setTags(without(configuration.getTags(), tagKeys));
            configurationStorage.put(arn, configuration);
            return;
        }
        MskVpcConnection connection = vpcConnectionStorage.get(arn).orElse(null);
        if (connection != null) {
            connection.setTags(without(connection.getTags(), tagKeys));
            vpcConnectionStorage.put(arn, connection);
            return;
        }
        throw taggedResourceNotFound(arn);
    }

    // Copies rather than mutating in place: a tag map that arrived through CreateCluster can be
    // an immutable Map (Map.of, or whatever Jackson handed us), which putAll/remove would
    // reject at runtime.
    private Map<String, String> merged(Map<String, String> existing, Map<String, String> added) {
        Map<String, String> result = new HashMap<>(existing != null ? existing : Map.of());
        if (added != null) {
            result.putAll(added);
        }
        return result;
    }

    private Map<String, String> without(Map<String, String> existing, List<String> tagKeys) {
        Map<String, String> result = new HashMap<>(existing != null ? existing : Map.of());
        if (tagKeys != null) {
            tagKeys.forEach(result::remove);
        }
        return result;
    }

    private AwsException taggedResourceNotFound(String arn) {
        return new AwsException("NotFoundException", "Resource " + arn + " does not exist.", 404);
    }

    private void startReadinessPoller() {
        poller.scheduleAtFixedRate(() -> {
            try {
                for (MskCluster cluster : allClusters()) {
                    if (cluster.getState() == ClusterState.CREATING && !config.services().msk().mock()) {
                        if (redpandaManager.isReady(cluster)) {
                            LOG.infov("MSK Cluster {0} is now ACTIVE", cluster.getClusterName());
                            cluster.setState(ClusterState.ACTIVE);
                            putCluster(cluster);
                        }
                    }
                }
                if (frontEnd != null) {
                    frontEnd.sync(allClusters());
                }
            } catch (Exception e) {
                LOG.error("Error in MSK readiness poller", e);
            }
        }, 1, 2, TimeUnit.SECONDS);
    }

    private List<MskCluster> allClusters() {
        if (storage instanceof AccountAwareStorageBackend<MskCluster> aware) {
            return aware.scanAllAccounts();
        }
        return storage.scan(k -> true);
    }

    public Optional<MskCluster> getClusterForAccount(String accountId, String clusterArn) {
        if (storage instanceof AccountAwareStorageBackend<MskCluster> aware) {
            return aware.getForAccount(accountId, clusterArn);
        }
        return storage.get(clusterArn);
    }

    private void putCluster(MskCluster cluster) {
        if (cluster.getAccountId() != null && storage instanceof AccountAwareStorageBackend<MskCluster> aware) {
            aware.putForAccount(cluster.getAccountId(), cluster.getClusterArn(), cluster);
        } else {
            storage.put(cluster.getClusterArn(), cluster);
        }
    }

    @Override
    public List<ExplorerResource> getResources() {
        List<ExplorerResource> resources = new ArrayList<>();
        for (MskCluster cluster : storage.scan(k -> true).stream().filter(this::isCurrentRegion).toList()) {
            String arn = cluster.getClusterArn();
            if (arn == null) {
                continue;
            }
            AwsArnUtils.Arn parsed = AwsArnUtils.parse(arn);
            resources.add(new ExplorerResource(
                    arn, "kafka:cluster", "kafka",
                    parsed.region(), parsed.accountId(),
                    cluster.getCreationTime() != null ? cluster.getCreationTime() : Instant.now(),
                    cluster.getTags() != null ? cluster.getTags() : Map.of()));
        }
        return resources;
    }

    @Override
    public Set<SupportedResourceType> getSupportedResourceTypes() {
        return Set.of(new SupportedResourceType("kafka:cluster", "kafka", true));
    }

    /**
     * Configurations are regional like clusters. Their ARN has always carried the region they were
     * created in, so it decides; there are no older records to keep visible.
     */
    private boolean isCurrentRegion(MskConfiguration configuration) {
        return regionResolver.getRegion().equals(AwsArnUtils.regionOrDefault(configuration.getArn(), null));
    }

    private boolean isCurrentRegion(MskCluster cluster) {
        if (cluster.getResourceRegion() != null) {
            return regionResolver.getRegion().equals(cluster.getResourceRegion());
        }
        // Older records have no request-region marker and their ARN always used the configured
        // default. Keep them visible until they are recreated so an upgrade does not hide or
        // strand persisted clusters whose original request region was not stored.
        return true;
    }
}
