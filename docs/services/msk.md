# MSK (Managed Streaming for Kafka)

**Protocol:** REST-JSON
**Endpoint:** `http://localhost:4566/`

Floci emulates Amazon MSK by orchestrating **Redpanda** containers. This provides high compatibility with the Kafka API while maintaining a low footprint.

## Supported Actions

| Action | Description |
|---|---|
| `CreateCluster` | Spawns a new Redpanda container for the cluster |
| `CreateClusterV2` | Create a provisioned or serverless cluster |
| `ListClusters` | List all emulated clusters |
| `ListClustersV2` | List all emulated clusters using V2 API |
| `DescribeCluster` | Get cluster metadata and state |
| `DescribeClusterV2` | Get cluster metadata and state using V2 API |
| `DeleteCluster` | Stops and removes the Redpanda container |
| `GetBootstrapBrokers` | Get the connection strings for the cluster, including the TLS and IAM listeners |
| `UpdateConnectivity` | Turn on multi-VPC private connectivity (or public access); completes at once |
| `DescribeClusterOperation` | Report an operation, always `UPDATE_COMPLETE` |
| `PutClusterPolicy` / `GetClusterPolicy` / `DeleteClusterPolicy` | Store the cluster's resource policy with a version; not evaluated |
| `CreateVpcConnection` / `DescribeVpcConnection` / `ListVpcConnections` / `DeleteVpcConnection` | Multi-VPC connections, from any account, to a cluster by ARN |
| `ListClientVpcConnections` | The VPC connections made to a cluster |
| `CreateTopic` / `DescribeTopic` / `ListTopics` / `UpdateTopic` / `DeleteTopic` | Topics, created on the broker with `rpk` |
| `BatchAssociateScramSecret` | Associate secret SCRAM credential with cluster |
| `ListScramSecrets` | List the secret ARNs associated with a cluster |
| `BatchDisassociateScramSecret` | Remove secret SCRAM credential associated with cluster |
| `CreateConfiguration` | Create a broker configuration (`server.properties`) |
| `ListConfigurations` | List all configurations |
| `DescribeConfiguration` | Get configuration metadata and latest revision |
| `DeleteConfiguration` | Delete a configuration |
| `UpdateConfiguration` | Create a new revision of a configuration |
| `ListConfigurationRevisions` | List all revisions of a configuration |
| `DescribeConfigurationRevision` | Get a specific revision, including its `server.properties` |
| `ListTagsForResource` | List the tags on a cluster or configuration |
| `TagResource` | Add tags to a cluster or configuration |
| `UntagResource` | Remove tags from a cluster or configuration |

## Configuration

| Variable | Default | Description |
|---|---|---|
| `FLOCI_SERVICES_MSK_ENABLED` | `true` | Enable or disable the service |
| `FLOCI_SERVICES_MSK_MOCK` | `false` | `true` = metadata-only CRUD, no Docker containers |
| `FLOCI_SERVICES_MSK_DEFAULT_IMAGE` | `redpandadata/redpanda:latest` | Docker image for Redpanda (Kafka) containers |
| `FLOCI_SERVICES_MSK_KAFKA_HOST_PORT_BASE` | `9300` | First host port a broker's Kafka listener can be published on |
| `FLOCI_SERVICES_MSK_KAFKA_HOST_PORT_MAX` | `9399` | Last host port a broker's Kafka listener can be published on |
| `FLOCI_SERVICES_MSK_SCRAM_RECONCILIATION_INTERVAL_MS` | `5000` | SCRAM secret reconciliation interval (ms) |
| `FLOCI_SERVICES_MSK_BOOTSTRAP_HOSTNAME` | _(unset)_ | Floci in a container only: hostname the host listener advertises, and when set, `GetBootstrapBrokers` returns `<container-name>:9092,<hostname>:<host port>` instead of the Docker-network address |

## How it works

When `mock` is set to `false` (default), Floci uses the Docker API to start a Redpanda container for each created cluster. For Docker socket setup, private registry authentication, and other Docker settings see [Docker Configuration](../configuration/docker.md).

- **Networking**: Kafka clients connect to whatever address the broker advertises, so the advertised address has to be reachable from the client.
    - Floci on the host: the Kafka API (9092) is published on a host port from the `kafka-host-port` range and advertised as `localhost:<port>`.
    - Floci in a container: the broker has two listeners. `internal` (9092) advertises `<container-name>:9092` for containers on the same Docker network. `host` (9093) is published on a host port from the `kafka-host-port` range and advertised as `<bootstrap-hostname or localhost>:<port>` for clients on the Docker host. `GetBootstrapBrokers` returns the Docker-network address. When `FLOCI_SERVICES_MSK_BOOTSTRAP_HOSTNAME` is set, it returns both listeners, `<container-name>:9092,<hostname>:<port>`: a client connects through whichever entry it can reach, and the broker then advertises that listener's address. The two entries are two listeners on the same broker, not two brokers.
- **Persistence**: Each cluster gets a named Docker volume (`floci-msk-{volumeId}`). In memory mode the volume is removed on cluster delete; in persistent modes it is retained unless `FLOCI_STORAGE_PRUNE_VOLUMES_ON_DELETE=true`.
- **Readiness**: The cluster state transitions to `ACTIVE` once the Redpanda `/ready` endpoint is reachable.

## TLS and IAM listeners

A cluster with TLS in transit gets the listeners MSK gives it, served on Floci's address by a front
end in front of its Redpanda broker:

| Listener | Bootstrap member | Host and port |
|---|---|---|
| TLS, no authentication | `BootstrapBrokerStringTls` | `b-N.<cluster>.<id>.c1.kafka.<region>.amazonaws.com:9094` |
| SASL/IAM | `BootstrapBrokerStringSaslIam` | `b-N.<cluster>.<id>.c1.kafka.<region>.amazonaws.com:9098` |
| Multi-VPC SASL/IAM | `BootstrapBrokerStringVpcConnectivitySaslIam` | `b-N.iam.<cluster>.<id>.c1.kafka.<region>.amazonaws.com:1400N` |

The TLS listener is there when unauthenticated or TLS client authentication is on, the IAM one when
`clientAuthentication.sasl.iam` is on, and the multi-VPC one when
`connectivityInfo.vpcConnectivity` enables IAM. `N` runs over `numberOfBrokerNodes`.

- **DNS**: the embedded DNS server resolves every broker name to Floci, for ECS tasks and any other
  container that uses it. In AWS the multi-VPC names resolve only in a VPC with a connection; here
  they resolve everywhere.
- **TLS** is terminated with a certificate the Floci root CA issues for the cluster's broker names,
  chosen by SNI. Clients trust it through the CA bundle (ECS tasks get it with
  `FLOCI_TLS_ENABLED=true`, as `NODE_EXTRA_CA_CERTS` and friends) or `GET /_floci/ca.pem`.
- **SASL**: `AWS_MSK_IAM` (as `@jm18457/kafkajs-msk-iam-authentication-mechanism` and the AWS MSK
  IAM libraries send it) and `OAUTHBEARER` (an `aws-msk-iam-sasl-signer` token) are answered by the
  front end. A payload is accepted when it is well formed: version `2020_10_22`, action
  `kafka-cluster:Connect`, a `kafka-cluster` credential scope, a date and a signature. The signature
  is not verified and IAM policies are not evaluated, so any signed client is let in. A client that
  sends anything but `ApiVersions` before authenticating on an IAM listener is disconnected.
- **Brokers**: every broker name leads to the cluster's single Redpanda node. Metadata and
  FindCoordinator responses advertise broker 1 of the listener the client came in on, so a client
  keeps using that listener family. The front end caps `Metadata` at v8 and `FindCoordinator` at v3
  in its `ApiVersions` answer, which clients negotiate down to.
- Topics created through `CreateTopic` get one replica on the broker, whatever factor was asked for;
  the requested factor is what the API reports. `ListTopics` and `DescribeTopic` report the topics
  the topic API manages, not ones a client created on the broker directly. `ListTopics`,
  `ListVpcConnections` and `ListClientVpcConnections` return every result in one page.
- **Ports**: the front end binds 9094, 9098 and 14001 upward on every interface of the Floci host
  while an active cluster has a listener on them, and closes a port once none does. The ports are
  fixed, as MSK's are; `FLOCI_SERVICES_MSK_MOCK=true` binds none.

## Cluster metadata

`CreateCluster` and `CreateClusterV2` persist the metadata you pass — broker node group
(instance type, subnets, security groups, storage, connectivity), number of broker nodes,
encryption, client authentication, enhanced monitoring, open monitoring, logging,
configuration, storage mode, rebalancing and tags —
and echo it back from `DescribeCluster`/`DescribeClusterV2` and the matching `List` calls.

The two API versions return different shapes, matching AWS:

- **v1** (`DescribeCluster`) returns a flat `ClusterInfo`: `brokerNodeGroupInfo`,
  `encryptionInfo`, `clientAuthentication`, `enhancedMonitoring`, `loggingInfo`,
  `openMonitoring`, `storageMode`, `rebalancing`, `numberOfBrokerNodes` and
  `zookeeperConnectString` all sit directly on the cluster.
- **v2** (`DescribeClusterV2`) nests all of those under `ClusterInfo.Provisioned`, alongside a
  top-level `ClusterType` of `PROVISIONED`. `ClusterArn`, `ClusterName`, `State`,
  `CreationTime`, `CurrentVersion` and `Tags` stay top-level.

Two details worth knowing:

- `configurationInfo` is a *request-only* member. The configuration a cluster was created
  with is reported back on `currentBrokerSoftwareInfo` as `configurationArn` and
  `configurationRevision`, which is where the Terraform AWS provider reads it from.
- Members AWS defaults server-side are defaulted here too, so a `terraform plan` converges:
  `enhancedMonitoring` becomes `DEFAULT`, and `encryptionInfo.encryptionInTransit` becomes
  `clientBroker: TLS_PLAINTEXT` / `inCluster: true` when the request omits them.

### Serverless clusters

`CreateClusterV2` accepts a `serverless` member as well as `provisioned` — exactly one of the
two, as on AWS. A serverless cluster stores its `vpcConfigs` and `clientAuthentication`, reports
`clusterType: SERVERLESS`, and comes back from `DescribeClusterV2` under a `serverless` envelope
rather than a `provisioned` one. It is backed by the same emulated Kafka endpoint, so
`GetBootstrapBrokers` works normally.

The v1 API predates serverless and its `ClusterInfo` cannot represent one, so — as on AWS —
`DescribeCluster` on a serverless cluster returns `BadRequestException` pointing you at
`DescribeClusterV2`, and `ListClusters` omits serverless clusters entirely.

### Validation

`CreateCluster`/`CreateClusterV2` reject the values AWS rejects: a missing or over-64-character
`clusterName`, a `numberOfBrokerNodes` below 1, an EBS `volumeSize` outside 1–16384, a
`configurationInfo.revision` below 1, and any unrecognised `enhancedMonitoring`, `storageMode`,
`rebalancing.status` or `encryptionInTransit.clientBroker` value. Each 400 names the offending
member in `invalidParameter`, as MSK's `Error` schema does.

There is deliberately **no** upper bound on `numberOfBrokerNodes`. The SDK model caps it at 15,
but the REST API reference documents no maximum for it — on a page that gives ranges for its
neighbours — and the quota page allows 30 brokers per ZooKeeper cluster and 60 per KRaft
cluster, both adjustable. A 30-broker cluster is an ordinary thing to have.

Unlike AWS, the *presence* of `kafkaVersion`, `numberOfBrokerNodes` and `brokerNodeGroupInfo` is
not required — the emulator defaults them so that a minimal create request still works.

### Tags

Cluster and configuration tags are managed through `/v1/tags/{resourceArn}`
(`ListTagsForResource`, `TagResource`, `UntagResource`). Cluster tags also round-trip through
`DescribeCluster`/`DescribeClusterV2`, so a tag change is visible to a refresh either way.
Configuration tags are reachable only through the tag endpoints, because AWS's
`DescribeConfiguration` response has no tags member.

### SCRAM Secrets Association 

Manages credentials only: the broker does not enable SASL authentication or
validate these users when clients connect. Clients can still connect without
credentials. Authenticated SASL_SSL listeners is not implemented.

`BatchAssociateScramSecret` accepts up to 10 Secrets Manager ARNs and creates kafka
`SCRAM-SHA-512` users.
Secrets must belong to the cluster's account, region and partition, use the `AmazonMSK_`
name prefix and a customer-managed symmetric KMS key ID, not an `alias/...` reference.
The secrets must contain JSON such as `{"username":"alice","password":"pw"}`.

`ListScramSecrets` supports pagination. `BatchDisassociateScramSecret` removes up to
10 associations. Item failures appear in
`unprocessedScramSecrets`; reassociation and removal of an unassociated ARN return
`InvalidSecretArn`.

Floci schedules SCRAM reconciliation jobs every five seconds by default, measured as
a delay after each scheduling sweep. Sweeps do not wait for broker operations to finish,
and clusters with queued or running jobs are skipped. Set
`FLOCI_SERVICES_MSK_SCRAM_RECONCILIATION_INTERVAL_MS` to change this delay in milliseconds.
This is Floci's timing, not an AWS synchronization guarantee.

When an associated secret is scheduled for deletion or permanently deleted, reconciliation
revokes its broker user but retains the association in `ListScramSecrets`. Restoring a secret
whose deletion was scheduled causes reconciliation to recreate its broker user using the
current credentials, without requiring reassociation. Permanently deleted secrets cannot
be restored; creating a secret with the same name produces a different ARN and requires
a new association.

## Examples

```bash
export AWS_ENDPOINT_URL=http://localhost:4566

# Create a cluster
aws kafka create-cluster \
  --cluster-name my-cluster \
  --kafka-version "3.6.1" \
  --number-of-broker-nodes 1 \
  --broker-node-group-info '{"InstanceType":"kafka.m5.large","ClientSubnets":["subnet-1"]}' \
  --endpoint-url $AWS_ENDPOINT_URL

# List clusters
aws kafka list-clusters --endpoint-url $AWS_ENDPOINT_URL

# Get bootstrap brokers
CLUSTER_ARN=$(aws kafka list-clusters --query 'ClusterInfoList[0].ClusterArn' --output text --endpoint-url $AWS_ENDPOINT_URL)
aws kafka get-bootstrap-brokers --cluster-arn $CLUSTER_ARN --endpoint-url $AWS_ENDPOINT_URL

# Associate an existing secret
aws kafka batch-associate-scram-secret \
  --cluster-arn "$CLUSTER_ARN" \
  --secret-arn-list "arn:aws:secretsmanager:us-east-1:000000000000:secret:AmazonMSK_my-user" \
  --endpoint-url $AWS_ENDPOINT_URL

# Delete a cluster
aws kafka delete-cluster --cluster-arn $CLUSTER_ARN --endpoint-url $AWS_ENDPOINT_URL
```
