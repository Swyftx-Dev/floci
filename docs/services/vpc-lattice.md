# Amazon VPC Lattice

**Protocol:** REST JSON
**Endpoint:** `http://localhost:4566`
**Signing name:** `vpc-lattice` (control plane), `vpc-lattice-svcs` (requests to a service)
**API version:** `2022-11-30`

Floci emulates the VPC Lattice control plane for service networks, services, target groups,
listeners and their associations, and runs a data plane: every listener is served on Floci's own
address, picks the service by Host header, and forwards to the healthy targets of its target
group. ECS services with `vpcLatticeConfigurations` register their tasks as targets, so tasks
reach each other through a service's generated name or its custom domain.

## Supported Actions

<!-- floci:actions:start -->
| Action | Description |
| --- | --- |
| `CreateServiceNetwork` | Creates a service network with its auth type and sharing config |
| `GetServiceNetwork` | Returns a service network by ID or ARN, also one in another account |
| `ListServiceNetworks` | Lists the caller's service networks in the Region |
| `UpdateServiceNetwork` | Changes the auth type |
| `DeleteServiceNetwork` | Rejected with `ConflictException` while associations remain |
| `CreateService` | Creates a service in `ACTIVE` state with its generated DNS entry |
| `GetService` | Returns a service by ID or ARN |
| `ListServices` | Lists the caller's services in the Region |
| `UpdateService` | Changes the auth type, certificate or idle timeout |
| `DeleteService` | Deletes the service and its listeners; rejected while it is associated |
| `CreateTargetGroup` | Creates a target group, filling the health check defaults AWS applies |
| `GetTargetGroup` | Returns a target group and the services whose listeners use it |
| `ListTargetGroups` | Lists target groups, optionally by VPC and type |
| `UpdateTargetGroup` | Merges a health check change into the stored one |
| `DeleteTargetGroup` | Rejected with `ConflictException` while a listener forwards to it |
| `RegisterTargets` | Registers targets; the port defaults to the target group's |
| `DeregisterTargets` | Removes targets |
| `ListTargets` | Returns each target with its health status |
| `CreateListener` | Creates an HTTP, HTTPS or TLS_PASSTHROUGH listener with a default action |
| `GetListener` | Returns a listener |
| `ListListeners` | Lists a service's listeners |
| `UpdateListener` | Replaces the default action |
| `DeleteListener` | Deletes a listener and stops serving it |
| `CreateServiceNetworkVpcAssociation` | Associates a VPC, keeping private DNS settings and security groups |
| `GetServiceNetworkVpcAssociation` | Returns a VPC association |
| `ListServiceNetworkVpcAssociations` | Lists VPC associations by network or VPC |
| `UpdateServiceNetworkVpcAssociation` | Replaces the security groups |
| `DeleteServiceNetworkVpcAssociation` | Deletes a VPC association |
| `CreateServiceNetworkServiceAssociation` | Publishes a service on a network, also one shared from another account |
| `GetServiceNetworkServiceAssociation` | Returns a service association with the service's DNS entry |
| `ListServiceNetworkServiceAssociations` | Lists service associations by network or service |
| `DeleteServiceNetworkServiceAssociation` | Deletes a service association |
| `PutAuthPolicy` | Stores an auth policy on a service or service network |
| `GetAuthPolicy` | Returns the stored policy and whether it is active |
| `DeleteAuthPolicy` | Deletes an auth policy |
| `CreateAccessLogSubscription` | Stores an access log subscription |
| `GetAccessLogSubscription` | Returns an access log subscription |
| `ListAccessLogSubscriptions` | Lists the subscriptions of a service or service network |
| `UpdateAccessLogSubscription` | Changes the destination |
| `DeleteAccessLogSubscription` | Deletes an access log subscription |
| `ListTagsForResource` | Returns the tags of any Lattice resource by ARN |
| `TagResource` | Adds or replaces tags |
| `UntagResource` | Removes tag keys |
<!-- floci:actions:end -->

Rules (`CreateRule` and the other rule operations), resource configurations, resource gateways,
domain verifications and resource policies are not implemented.

## Data plane

Every listener port is served on Floci's address. A request is matched to a service by its Host
header, the service's generated `dnsEntry.domainName` or its `customDomainName`, then to the
service's listener on the port it arrived on, and the default action runs:

- `forward` picks a target group by weight and round-robins over its targets. Targets a health
  check has marked `UNHEALTHY` are skipped unless every target is unhealthy. No target answers 503.
- `fixedResponse` answers with its status code.
- A Host header no service claims answers 404.

The Host header reaches the target unchanged, as on AWS.

Lattice listener ports share their socket with ELBv2 listeners on the same port. A connection whose
Host header (or, over TLS, whose SNI name) names a Lattice service goes to Lattice; anything else
reaches the load balancer untouched, which terminates its own HTTPS. So an ALB and Lattice services
can both serve 443. A connection is routed by its first request, so keep-alive requests on it stay
with the same data plane. Floci's own port is never served, and with TLS enabled neither is
`floci.tls.aws-https-port` (443 by default): set `FLOCI_TLS_AWS_HTTPS_PORT=0` to give 443 to
listeners.

### TLS

Each port accepts TLS and plain HTTP on the same socket, told apart by the first byte. TLS is
terminated with a certificate chosen by SNI:

| Name | Certificate |
|---|---|
| The generated DNS name | Issued by the Floci root CA for that name |
| The custom domain | The service's `certificateArn`, read from ACM. An ACM private certificate (one requested with `CertificateAuthorityArn`) is issued by the Floci root CA |

A client trusts both by trusting the Floci root CA, served at `GET /_floci/ca.pem` and stored as
`tls/floci-root-ca.crt` under the persistent path. With `FLOCI_TLS_ENABLED=true`,
ECS task containers are started with `SSL_CERT_FILE`, `NODE_EXTRA_CA_CERTS`, `REQUESTS_CA_BUNDLE`
and `AWS_CA_BUNDLE` pointing at a bundle that already contains it. HTTPS to a service on port 443
then needs `FLOCI_TLS_AWS_HTTPS_PORT=0`, so the port is free for listeners.

### From ECS tasks

AWS answers a Lattice name with a link-local address in `169.254.171.0/24` inside a VPC, and
outbound proxies (an envoy sidecar with an iptables init container redirecting that range to it)
depend on that. Floci does the same for ECS tasks:

- An `awsvpc` task runs in one shared network namespace (see
  [ECS](ecs.md#awsvpc-task-network-namespace)), so the init container's iptables rules apply to the
  application, and `linuxParameters.capabilities` grants it `NET_ADMIN`.
- A query from a task's namespace for a service's generated name or custom domain is answered with
  `169.254.171.1`. Every other client still gets Floci's own address.
- The link-local proxy container on the task network (the one holding `169.254.170.2` for task-role
  credentials) also holds `169.254.171.1` and relays its ports 80 and 443 to the Lattice listeners
  on Floci. Task-role credentials keep working beside it: `169.254.170.2` is outside the redirected range.

So an application calling `http://<custom-domain>/` through such a proxy reaches the target signed
with the task role and over TLS on 443, and one calling `https://<custom-domain>/` itself reaches
it directly. This needs `FLOCI_SERVICES_ECS_DOCKER_NETWORK` set and security-group enforcement
off. Lattice listener ports other than 80 and 443 are not relayed on the link-local address.

### Auth

A service whose `authType` is `AWS_IAM`, or that is associated with a service network whose
`authType` is `AWS_IAM`, refuses a request without a SigV4 `Authorization` header (or a presigned
`X-Amz-Signature`) with 403 `AccessDeniedException`. The signature is not verified and auth
policies are stored but not evaluated: any signed request is allowed.

### Health checks

A target group whose health check is enabled probes each target with HTTP GET on the health
check path and port, at the configured interval, and moves the target to `HEALTHY` or
`UNHEALTHY` after the configured thresholds, matching the status code against `matcher.httpCode`.
`ListTargets` reports `UNUSED` for a target group no listener forwards to, `UNAVAILABLE` when the
health check is disabled, and `INITIAL` until the first verdict.

### DNS

The generated name is `<name>-<id>.<hash>.vpc-lattice-svcs.<region>.<floci-hostname>`, under
Floci's own DNS suffix (`FLOCI_HOSTNAME`, or `localhost.floci.io`), so the embedded DNS server
resolves it to Floci from every container it serves, and a Route 53 alias record to it resolves the
same way. `dnsEntry.hostedZoneId` is a fixed value per Region for alias records to target.

A custom domain resolves to Floci once its service is associated with a service network that has a
VPC association with `privateDnsEnabled`, when the association's `dnsOptions` allow the name:
`ALL_DOMAINS` (or no `dnsOptions`), or a name matching `privateDnsSpecifiedDomains`. Domain
verification is not emulated, so `VERIFIED_DOMAINS_ONLY` resolves nothing. The answer is the same
from every VPC.

## ECS

An ECS service's `vpcLatticeConfigurations` are resolved through the task definition: each entry's
`portName` names a port mapping, and every running task is registered into the entry's target group
at the address and port Floci reaches that container on, the same address an ELBv2 target gets.
Stopping the task deregisters it. `DescribeServices` reports the configurations on the primary
deployment, where AWS does.

## Not emulated

- Listener rules: only the default action routes.
- Auth policy evaluation and signature verification.
- Access logs: subscriptions are stored, nothing is delivered.
- `TLS_PASSTHROUGH` listeners are stored but not served.
- HTTP/2 and gRPC to targets: requests are forwarded over HTTP/1.1.
- Idle timeouts, `clientToken` idempotency.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `FLOCI_SERVICES_VPCLATTICE_ENABLED` | `true` | Enable or disable VPC Lattice |
| `FLOCI_SERVICES_VPCLATTICE_MOCK` | `false` | Run the control plane only: no listener ports, health checks or DNS answers for custom domains are served |

## Example

```bash
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

svc=$(aws vpc-lattice create-service --name orders --auth-type AWS_IAM --query id --output text)
tg=$(aws vpc-lattice create-target-group --name orders-8080 --type IP \
  --config '{"port":8080,"protocol":"HTTP","vpcIdentifier":"vpc-123"}' --query id --output text)
aws vpc-lattice create-listener --service-identifier "$svc" --name http --protocol HTTP --port 80 \
  --default-action "{\"forward\":{\"targetGroups\":[{\"targetGroupIdentifier\":\"$tg\",\"weight\":100}]}}"
aws vpc-lattice register-targets --target-group-identifier "$tg" --targets id=172.18.0.5,port=8080

name=$(aws vpc-lattice get-service --service-identifier "$svc" --query dnsEntry.domainName --output text)
curl --aws-sigv4 "aws:amz:us-east-1:vpc-lattice-svcs" --user test:test "http://$name/"
```
