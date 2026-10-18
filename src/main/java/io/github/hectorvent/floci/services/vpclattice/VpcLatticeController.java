package io.github.hectorvent.floci.services.vpclattice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.core.common.PaginatedResult;
import io.github.hectorvent.floci.core.common.Pagination;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.services.vpclattice.model.AccessLogSubscription;
import io.github.hectorvent.floci.services.vpclattice.model.AuthPolicy;
import io.github.hectorvent.floci.services.vpclattice.model.DnsOptions;
import io.github.hectorvent.floci.services.vpclattice.model.HealthCheckConfig;
import io.github.hectorvent.floci.services.vpclattice.model.LatticeService;
import io.github.hectorvent.floci.services.vpclattice.model.Listener;
import io.github.hectorvent.floci.services.vpclattice.model.RuleAction;
import io.github.hectorvent.floci.services.vpclattice.model.ServiceNetwork;
import io.github.hectorvent.floci.services.vpclattice.model.ServiceNetworkServiceAssociation;
import io.github.hectorvent.floci.services.vpclattice.model.ServiceNetworkVpcAssociation;
import io.github.hectorvent.floci.services.vpclattice.model.SharingConfig;
import io.github.hectorvent.floci.services.vpclattice.model.Target;
import io.github.hectorvent.floci.services.vpclattice.model.TargetGroup;
import io.github.hectorvent.floci.services.vpclattice.model.TargetGroupConfig;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@Path(VpcLatticeRouteFilter.INTERNAL_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.WILDCARD)
public class VpcLatticeController {

    private static final int MAX_PAGE = 100;

    private final VpcLatticeService service;
    private final RegionResolver regionResolver;
    private final ObjectMapper objectMapper;

    @Inject
    public VpcLatticeController(VpcLatticeService service, RegionResolver regionResolver, ObjectMapper objectMapper) {
        this.service = service;
        this.regionResolver = regionResolver;
        this.objectMapper = objectMapper;
    }

    @POST
    @Path("/servicenetworks")
    public Response createServiceNetwork(@Context HttpHeaders headers, String body) {
        JsonNode request = parse(body);
        ServiceNetwork sn = service.createServiceNetwork(text(request, "name"), text(request, "authType"),
                convert(request.get("sharingConfig"), SharingConfig.class), tags(request), region(headers));
        ObjectNode response = objectMapper.createObjectNode();
        response.put("id", sn.getId());
        response.put("name", sn.getName());
        response.put("arn", sn.getArn());
        putPojo(response, "sharingConfig", sn.getSharingConfig());
        response.put("authType", sn.getAuthType());
        return Response.status(201).entity(response).build();
    }

    @GET
    @Path("/servicenetworks/{id:.+}")
    public Response getServiceNetwork(@Context HttpHeaders headers, @PathParam("id") String id) {
        ServiceNetwork sn = service.getServiceNetwork(decode(id), region(headers));
        ObjectNode response = serviceNetworkSummary(sn);
        response.put("authType", sn.getAuthType());
        putPojo(response, "sharingConfig", sn.getSharingConfig());
        return Response.ok(response).build();
    }

    @GET
    @Path("/servicenetworks")
    public Response listServiceNetworks(@Context HttpHeaders headers, @QueryParam("maxResults") Integer maxResults,
                                        @QueryParam("nextToken") String nextToken) {
        return page(service.listServiceNetworks(region(headers)), ServiceNetwork::getId, maxResults, nextToken,
                this::serviceNetworkSummary);
    }

    @PATCH
    @Path("/servicenetworks/{id:.+}")
    public Response updateServiceNetwork(@Context HttpHeaders headers, @PathParam("id") String id, String body) {
        ServiceNetwork sn = service.updateServiceNetwork(decode(id), text(parse(body), "authType"), region(headers));
        ObjectNode response = objectMapper.createObjectNode();
        response.put("id", sn.getId());
        response.put("name", sn.getName());
        response.put("arn", sn.getArn());
        response.put("authType", sn.getAuthType());
        return Response.ok(response).build();
    }

    @DELETE
    @Path("/servicenetworks/{id:.+}")
    public Response deleteServiceNetwork(@Context HttpHeaders headers, @PathParam("id") String id) {
        service.deleteServiceNetwork(decode(id), region(headers));
        return Response.noContent().build();
    }

    @POST
    @Path("/services")
    public Response createService(@Context HttpHeaders headers, String body) {
        JsonNode request = parse(body);
        LatticeService svc = service.createService(text(request, "name"), text(request, "authType"),
                text(request, "customDomainName"), text(request, "certificateArn"),
                integer(request, "idleTimeoutSeconds"), tags(request), region(headers));
        return Response.status(201).entity(serviceBody(svc)).build();
    }

    @GET
    @Path("/services/{id:.+}")
    public Response getService(@Context HttpHeaders headers, @PathParam("id") String id) {
        LatticeService svc = service.getService(decode(id), region(headers));
        ObjectNode response = serviceBody(svc);
        putTime(response, "createdAt", svc.getCreatedAt());
        putTime(response, "lastUpdatedAt", svc.getLastUpdatedAt());
        return Response.ok(response).build();
    }

    @GET
    @Path("/services")
    public Response listServices(@Context HttpHeaders headers, @QueryParam("maxResults") Integer maxResults,
                                 @QueryParam("nextToken") String nextToken) {
        return page(service.listServices(region(headers)), LatticeService::getId, maxResults, nextToken, svc -> {
            ObjectNode item = objectMapper.createObjectNode();
            item.put("id", svc.getId());
            item.put("name", svc.getName());
            item.put("arn", svc.getArn());
            putTime(item, "createdAt", svc.getCreatedAt());
            putTime(item, "lastUpdatedAt", svc.getLastUpdatedAt());
            putPojo(item, "dnsEntry", svc.getDnsEntry());
            putText(item, "customDomainName", svc.getCustomDomainName());
            item.put("status", svc.getStatus());
            return item;
        });
    }

    @PATCH
    @Path("/services/{id:.+}")
    public Response updateService(@Context HttpHeaders headers, @PathParam("id") String id, String body) {
        JsonNode request = parse(body);
        LatticeService svc = service.updateService(decode(id), text(request, "certificateArn"),
                text(request, "authType"), integer(request, "idleTimeoutSeconds"), region(headers));
        ObjectNode response = serviceBody(svc);
        response.remove("status");
        response.remove("dnsEntry");
        return Response.ok(response).build();
    }

    @DELETE
    @Path("/services/{id:.+}")
    public Response deleteService(@Context HttpHeaders headers, @PathParam("id") String id) {
        LatticeService svc = service.deleteService(decode(id), region(headers));
        ObjectNode response = objectMapper.createObjectNode();
        response.put("id", svc.getId());
        response.put("arn", svc.getArn());
        response.put("name", svc.getName());
        response.put("status", svc.getStatus());
        return Response.ok(response).build();
    }

    @POST
    @Path("/targetgroups")
    public Response createTargetGroup(@Context HttpHeaders headers, String body) {
        JsonNode request = parse(body);
        TargetGroup tg = service.createTargetGroup(text(request, "name"), text(request, "type"),
                convert(request.get("config"), TargetGroupConfig.class), tags(request), region(headers));
        return Response.status(201).entity(targetGroupBody(tg)).build();
    }

    @GET
    @Path("/targetgroups/{id:.+}")
    public Response getTargetGroup(@Context HttpHeaders headers, @PathParam("id") String id) {
        TargetGroup tg = service.getTargetGroup(decode(id), region(headers));
        ObjectNode response = targetGroupBody(tg);
        putTime(response, "createdAt", tg.getCreatedAt());
        putTime(response, "lastUpdatedAt", tg.getLastUpdatedAt());
        response.set("serviceArns", objectMapper.valueToTree(service.serviceArnsUsing(tg)));
        return Response.ok(response).build();
    }

    @GET
    @Path("/targetgroups")
    public Response listTargetGroups(@Context HttpHeaders headers, @QueryParam("maxResults") Integer maxResults,
                                     @QueryParam("nextToken") String nextToken,
                                     @QueryParam("vpcIdentifier") String vpcIdentifier,
                                     @QueryParam("targetGroupType") String targetGroupType) {
        return page(service.listTargetGroups(vpcIdentifier, targetGroupType, region(headers)), TargetGroup::getId,
                maxResults, nextToken, tg -> {
                    ObjectNode item = objectMapper.createObjectNode();
                    item.put("id", tg.getId());
                    item.put("arn", tg.getArn());
                    item.put("name", tg.getName());
                    item.put("type", tg.getType());
                    putTime(item, "createdAt", tg.getCreatedAt());
                    if (tg.getConfig() != null) {
                        putInt(item, "port", tg.getConfig().getPort());
                        putText(item, "protocol", tg.getConfig().getProtocol());
                        putText(item, "ipAddressType", tg.getConfig().getIpAddressType());
                        putText(item, "vpcIdentifier", tg.getConfig().getVpcIdentifier());
                        putText(item, "lambdaEventStructureVersion", tg.getConfig().getLambdaEventStructureVersion());
                    }
                    putTime(item, "lastUpdatedAt", tg.getLastUpdatedAt());
                    item.put("status", tg.getStatus());
                    item.set("serviceArns", objectMapper.valueToTree(service.serviceArnsUsing(tg)));
                    return item;
                });
    }

    @PATCH
    @Path("/targetgroups/{id:.+}")
    public Response updateTargetGroup(@Context HttpHeaders headers, @PathParam("id") String id, String body) {
        JsonNode request = parse(body);
        TargetGroup tg = service.updateTargetGroup(decode(id),
                convert(request.get("healthCheck"), HealthCheckConfig.class), region(headers));
        return Response.ok(targetGroupBody(tg)).build();
    }

    @DELETE
    @Path("/targetgroups/{id:.+}")
    public Response deleteTargetGroup(@Context HttpHeaders headers, @PathParam("id") String id) {
        TargetGroup tg = service.deleteTargetGroup(decode(id), region(headers));
        ObjectNode response = objectMapper.createObjectNode();
        response.put("id", tg.getId());
        response.put("arn", tg.getArn());
        response.put("status", tg.getStatus());
        return Response.ok(response).build();
    }

    @POST
    @Path("/targetgroups/{id:.+}/registertargets")
    public Response registerTargets(@Context HttpHeaders headers, @PathParam("id") String id, String body) {
        List<Target> targets = service.registerTargets(decode(id), targets(parse(body)), region(headers));
        return Response.ok(targetResult(targets)).build();
    }

    @POST
    @Path("/targetgroups/{id:.+}/deregistertargets")
    public Response deregisterTargets(@Context HttpHeaders headers, @PathParam("id") String id, String body) {
        List<Target> targets = service.deregisterTargets(decode(id), targets(parse(body)), region(headers));
        return Response.ok(targetResult(targets)).build();
    }

    @POST
    @Path("/targetgroups/{id:.+}/listtargets")
    public Response listTargets(@Context HttpHeaders headers, @PathParam("id") String id,
                                @QueryParam("maxResults") Integer maxResults,
                                @QueryParam("nextToken") String nextToken, String body) {
        List<VpcLatticeService.TargetSummary> summaries = service.listTargets(decode(id), targets(parse(body)),
                region(headers));
        return page(summaries, t -> t.id() + ":" + t.port(), maxResults, nextToken, t -> {
            ObjectNode item = objectMapper.createObjectNode();
            item.put("id", t.id());
            putInt(item, "port", t.port());
            item.put("status", t.status());
            putText(item, "reasonCode", t.reasonCode());
            return item;
        });
    }

    @POST
    @Path("/services/{serviceId:.+}/listeners")
    public Response createListener(@Context HttpHeaders headers, @PathParam("serviceId") String serviceId,
                                   String body) {
        JsonNode request = parse(body);
        Listener listener = service.createListener(decode(serviceId), text(request, "name"),
                text(request, "protocol"), integer(request, "port"),
                convert(request.get("defaultAction"), RuleAction.class), tags(request), region(headers));
        return Response.status(201).entity(listenerBody(listener)).build();
    }

    @GET
    @Path("/services/{serviceId:.+}/listeners/{listenerId:.+}")
    public Response getListener(@Context HttpHeaders headers, @PathParam("serviceId") String serviceId,
                                @PathParam("listenerId") String listenerId) {
        Listener listener = service.getListener(decode(serviceId), decode(listenerId), region(headers));
        ObjectNode response = listenerBody(listener);
        putTime(response, "createdAt", listener.getCreatedAt());
        putTime(response, "lastUpdatedAt", listener.getLastUpdatedAt());
        return Response.ok(response).build();
    }

    @GET
    @Path("/services/{serviceId:.+}/listeners")
    public Response listListeners(@Context HttpHeaders headers, @PathParam("serviceId") String serviceId,
                                  @QueryParam("maxResults") Integer maxResults,
                                  @QueryParam("nextToken") String nextToken) {
        return page(service.listListeners(decode(serviceId), region(headers)), Listener::getId, maxResults, nextToken,
                listener -> {
                    ObjectNode item = objectMapper.createObjectNode();
                    item.put("arn", listener.getArn());
                    item.put("id", listener.getId());
                    item.put("name", listener.getName());
                    item.put("protocol", listener.getProtocol());
                    putInt(item, "port", listener.getPort());
                    putTime(item, "createdAt", listener.getCreatedAt());
                    putTime(item, "lastUpdatedAt", listener.getLastUpdatedAt());
                    return item;
                });
    }

    @PATCH
    @Path("/services/{serviceId:.+}/listeners/{listenerId:.+}")
    public Response updateListener(@Context HttpHeaders headers, @PathParam("serviceId") String serviceId,
                                   @PathParam("listenerId") String listenerId, String body) {
        Listener listener = service.updateListener(decode(serviceId), decode(listenerId),
                convert(parse(body).get("defaultAction"), RuleAction.class), region(headers));
        return Response.ok(listenerBody(listener)).build();
    }

    @DELETE
    @Path("/services/{serviceId:.+}/listeners/{listenerId:.+}")
    public Response deleteListener(@Context HttpHeaders headers, @PathParam("serviceId") String serviceId,
                                   @PathParam("listenerId") String listenerId) {
        service.deleteListener(decode(serviceId), decode(listenerId), region(headers));
        return Response.noContent().build();
    }

    @POST
    @Path("/servicenetworkvpcassociations")
    public Response createServiceNetworkVpcAssociation(@Context HttpHeaders headers, String body) {
        JsonNode request = parse(body);
        ServiceNetworkVpcAssociation association = service.createVpcAssociation(
                text(request, "serviceNetworkIdentifier"), text(request, "vpcIdentifier"),
                bool(request, "privateDnsEnabled"), strings(request.get("securityGroupIds")),
                convert(request.get("dnsOptions"), DnsOptions.class), tags(request), region(headers));
        ObjectNode response = objectMapper.createObjectNode();
        response.put("id", association.getId());
        response.put("status", association.getStatus());
        response.put("arn", association.getArn());
        response.put("createdBy", association.getCreatedBy());
        putList(response, "securityGroupIds", association.getSecurityGroupIds());
        putBool(response, "privateDnsEnabled", association.getPrivateDnsEnabled());
        putPojo(response, "dnsOptions", association.getDnsOptions());
        return Response.ok(response).build();
    }

    @GET
    @Path("/servicenetworkvpcassociations/{id:.+}")
    public Response getServiceNetworkVpcAssociation(@Context HttpHeaders headers, @PathParam("id") String id) {
        ServiceNetworkVpcAssociation association = service.getVpcAssociation(decode(id), region(headers));
        ObjectNode response = vpcAssociationSummary(association);
        putList(response, "securityGroupIds", association.getSecurityGroupIds());
        return Response.ok(response).build();
    }

    @GET
    @Path("/servicenetworkvpcassociations")
    public Response listServiceNetworkVpcAssociations(@Context HttpHeaders headers,
                                        @QueryParam("serviceNetworkIdentifier") String serviceNetworkIdentifier,
                                        @QueryParam("vpcIdentifier") String vpcIdentifier,
                                        @QueryParam("maxResults") Integer maxResults,
                                        @QueryParam("nextToken") String nextToken) {
        return page(service.listVpcAssociations(serviceNetworkIdentifier, vpcIdentifier, region(headers)),
                ServiceNetworkVpcAssociation::getId, maxResults, nextToken, this::vpcAssociationSummary);
    }

    @PATCH
    @Path("/servicenetworkvpcassociations/{id:.+}")
    public Response updateServiceNetworkVpcAssociation(@Context HttpHeaders headers, @PathParam("id") String id,
                                                       String body) {
        ServiceNetworkVpcAssociation association = service.updateVpcAssociation(decode(id),
                strings(parse(body).get("securityGroupIds")), region(headers));
        ObjectNode response = objectMapper.createObjectNode();
        response.put("id", association.getId());
        response.put("arn", association.getArn());
        response.put("status", association.getStatus());
        response.put("createdBy", association.getCreatedBy());
        putList(response, "securityGroupIds", association.getSecurityGroupIds());
        return Response.ok(response).build();
    }

    @DELETE
    @Path("/servicenetworkvpcassociations/{id:.+}")
    public Response deleteServiceNetworkVpcAssociation(@Context HttpHeaders headers, @PathParam("id") String id) {
        ServiceNetworkVpcAssociation association = service.deleteVpcAssociation(decode(id), region(headers));
        ObjectNode response = objectMapper.createObjectNode();
        response.put("id", association.getId());
        response.put("status", association.getStatus());
        response.put("arn", association.getArn());
        return Response.ok(response).build();
    }

    @POST
    @Path("/servicenetworkserviceassociations")
    public Response createServiceNetworkServiceAssociation(@Context HttpHeaders headers, String body) {
        JsonNode request = parse(body);
        ServiceNetworkServiceAssociation association = service.createServiceAssociation(
                text(request, "serviceIdentifier"), text(request, "serviceNetworkIdentifier"), tags(request),
                region(headers));
        ObjectNode response = objectMapper.createObjectNode();
        response.put("id", association.getId());
        response.put("status", association.getStatus());
        response.put("arn", association.getArn());
        response.put("createdBy", association.getCreatedBy());
        putServiceDns(response, association);
        return Response.ok(response).build();
    }

    @GET
    @Path("/servicenetworkserviceassociations/{id:.+}")
    public Response getServiceNetworkServiceAssociation(@Context HttpHeaders headers, @PathParam("id") String id) {
        return Response.ok(serviceAssociationSummary(service.getServiceAssociation(decode(id), region(headers))))
                .build();
    }

    @GET
    @Path("/servicenetworkserviceassociations")
    public Response listServiceNetworkServiceAssociations(@Context HttpHeaders headers,
                                            @QueryParam("serviceNetworkIdentifier") String serviceNetworkIdentifier,
                                            @QueryParam("serviceIdentifier") String serviceIdentifier,
                                            @QueryParam("maxResults") Integer maxResults,
                                            @QueryParam("nextToken") String nextToken) {
        return page(service.listServiceAssociations(serviceNetworkIdentifier, serviceIdentifier, region(headers)),
                ServiceNetworkServiceAssociation::getId, maxResults, nextToken, this::serviceAssociationSummary);
    }

    @DELETE
    @Path("/servicenetworkserviceassociations/{id:.+}")
    public Response deleteServiceNetworkServiceAssociation(@Context HttpHeaders headers, @PathParam("id") String id) {
        ServiceNetworkServiceAssociation association = service.deleteServiceAssociation(decode(id), region(headers));
        ObjectNode response = objectMapper.createObjectNode();
        response.put("id", association.getId());
        response.put("status", association.getStatus());
        response.put("arn", association.getArn());
        return Response.ok(response).build();
    }

    @PUT
    @Path("/authpolicy/{resource:.+}")
    public Response putAuthPolicy(@Context HttpHeaders headers, @PathParam("resource") String resource, String body) {
        String identifier = decode(resource);
        AuthPolicy policy = service.putAuthPolicy(identifier, text(parse(body), "policy"), region(headers));
        ObjectNode response = objectMapper.createObjectNode();
        response.put("policy", policy.getPolicy());
        response.put("state", service.authPolicyState(identifier, region(headers)));
        return Response.ok(response).build();
    }

    @GET
    @Path("/authpolicy/{resource:.+}")
    public Response getAuthPolicy(@Context HttpHeaders headers, @PathParam("resource") String resource) {
        String identifier = decode(resource);
        AuthPolicy policy = service.getAuthPolicy(identifier, region(headers));
        ObjectNode response = objectMapper.createObjectNode();
        response.put("policy", policy.getPolicy());
        response.put("state", service.authPolicyState(identifier, region(headers)));
        putTime(response, "createdAt", policy.getCreatedAt());
        putTime(response, "lastUpdatedAt", policy.getLastUpdatedAt());
        return Response.ok(response).build();
    }

    @DELETE
    @Path("/authpolicy/{resource:.+}")
    public Response deleteAuthPolicy(@Context HttpHeaders headers, @PathParam("resource") String resource) {
        service.deleteAuthPolicy(decode(resource), region(headers));
        return Response.noContent().build();
    }

    @POST
    @Path("/accesslogsubscriptions")
    public Response createAccessLogSubscription(@Context HttpHeaders headers, String body) {
        JsonNode request = parse(body);
        AccessLogSubscription subscription = service.createAccessLogSubscription(text(request, "resourceIdentifier"),
                text(request, "destinationArn"), text(request, "serviceNetworkLogType"), tags(request),
                region(headers));
        ObjectNode response = accessLogSubscriptionBody(subscription);
        response.remove("createdAt");
        response.remove("lastUpdatedAt");
        return Response.status(201).entity(response).build();
    }

    @GET
    @Path("/accesslogsubscriptions/{id:.+}")
    public Response getAccessLogSubscription(@Context HttpHeaders headers, @PathParam("id") String id) {
        return Response.ok(accessLogSubscriptionBody(service.getAccessLogSubscription(decode(id), region(headers))))
                .build();
    }

    @GET
    @Path("/accesslogsubscriptions")
    public Response listAccessLogSubscriptions(@Context HttpHeaders headers,
                                               @QueryParam("resourceIdentifier") String resourceIdentifier,
                                               @QueryParam("maxResults") Integer maxResults,
                                               @QueryParam("nextToken") String nextToken) {
        return page(service.listAccessLogSubscriptions(resourceIdentifier, region(headers)),
                AccessLogSubscription::getId, maxResults, nextToken, this::accessLogSubscriptionBody);
    }

    @PATCH
    @Path("/accesslogsubscriptions/{id:.+}")
    public Response updateAccessLogSubscription(@Context HttpHeaders headers, @PathParam("id") String id,
                                                String body) {
        AccessLogSubscription subscription = service.updateAccessLogSubscription(decode(id),
                text(parse(body), "destinationArn"), region(headers));
        ObjectNode response = accessLogSubscriptionBody(subscription);
        response.remove("createdAt");
        response.remove("lastUpdatedAt");
        response.remove("serviceNetworkLogType");
        return Response.ok(response).build();
    }

    @DELETE
    @Path("/accesslogsubscriptions/{id:.+}")
    public Response deleteAccessLogSubscription(@Context HttpHeaders headers, @PathParam("id") String id) {
        service.deleteAccessLogSubscription(decode(id), region(headers));
        return Response.noContent().build();
    }

    @GET
    @Path("/tags/{resourceArn:.+}")
    public Response listTagsForResource(@PathParam("resourceArn") String resourceArn) {
        ObjectNode response = objectMapper.createObjectNode();
        response.set("tags", objectMapper.valueToTree(service.listTags(decode(resourceArn))));
        return Response.ok(response).build();
    }

    @POST
    @Path("/tags/{resourceArn:.+}")
    public Response tagResource(@PathParam("resourceArn") String resourceArn, String body) {
        service.tagResource(decode(resourceArn), tags(parse(body)));
        return Response.ok(objectMapper.createObjectNode()).build();
    }

    @DELETE
    @Path("/tags/{resourceArn:.+}")
    public Response untagResource(@PathParam("resourceArn") String resourceArn,
                                  @QueryParam("tagKeys") List<String> tagKeys) {
        service.untagResource(decode(resourceArn), tagKeys);
        return Response.ok(objectMapper.createObjectNode()).build();
    }

    private ObjectNode serviceNetworkSummary(ServiceNetwork sn) {
        ObjectNode item = objectMapper.createObjectNode();
        item.put("id", sn.getId());
        item.put("name", sn.getName());
        item.put("arn", sn.getArn());
        putTime(item, "createdAt", sn.getCreatedAt());
        putTime(item, "lastUpdatedAt", sn.getLastUpdatedAt());
        item.put("numberOfAssociatedVPCs", service.associatedVpcCount(sn));
        item.put("numberOfAssociatedServices", service.associatedServiceCount(sn));
        return item;
    }

    private ObjectNode serviceBody(LatticeService svc) {
        ObjectNode response = objectMapper.createObjectNode();
        response.put("id", svc.getId());
        response.put("arn", svc.getArn());
        response.put("name", svc.getName());
        putText(response, "customDomainName", svc.getCustomDomainName());
        putText(response, "certificateArn", svc.getCertificateArn());
        response.put("status", svc.getStatus());
        response.put("authType", svc.getAuthType());
        putInt(response, "idleTimeoutSeconds", svc.getIdleTimeoutSeconds());
        putPojo(response, "dnsEntry", svc.getDnsEntry());
        return response;
    }

    private ObjectNode targetGroupBody(TargetGroup tg) {
        ObjectNode response = objectMapper.createObjectNode();
        response.put("id", tg.getId());
        response.put("arn", tg.getArn());
        response.put("name", tg.getName());
        response.put("type", tg.getType());
        putPojo(response, "config", tg.getConfig());
        response.put("status", tg.getStatus());
        return response;
    }

    private ObjectNode listenerBody(Listener listener) {
        ObjectNode response = objectMapper.createObjectNode();
        response.put("arn", listener.getArn());
        response.put("id", listener.getId());
        response.put("name", listener.getName());
        response.put("protocol", listener.getProtocol());
        putInt(response, "port", listener.getPort());
        response.put("serviceArn", listener.getServiceArn());
        response.put("serviceId", listener.getServiceId());
        putPojo(response, "defaultAction", listener.getDefaultAction());
        return response;
    }

    private ObjectNode vpcAssociationSummary(ServiceNetworkVpcAssociation association) {
        ObjectNode item = objectMapper.createObjectNode();
        item.put("id", association.getId());
        item.put("arn", association.getArn());
        item.put("status", association.getStatus());
        item.put("createdBy", association.getCreatedBy());
        putTime(item, "createdAt", association.getCreatedAt());
        item.put("serviceNetworkId", association.getServiceNetworkId());
        item.put("serviceNetworkName", association.getServiceNetworkName());
        item.put("serviceNetworkArn", association.getServiceNetworkArn());
        putBool(item, "privateDnsEnabled", association.getPrivateDnsEnabled());
        putPojo(item, "dnsOptions", association.getDnsOptions());
        item.put("vpcId", association.getVpcId());
        putTime(item, "lastUpdatedAt", association.getLastUpdatedAt());
        return item;
    }

    private ObjectNode serviceAssociationSummary(ServiceNetworkServiceAssociation association) {
        ObjectNode item = objectMapper.createObjectNode();
        item.put("id", association.getId());
        item.put("status", association.getStatus());
        item.put("arn", association.getArn());
        item.put("createdBy", association.getCreatedBy());
        putTime(item, "createdAt", association.getCreatedAt());
        item.put("serviceId", association.getServiceId());
        item.put("serviceName", association.getServiceName());
        item.put("serviceArn", association.getServiceArn());
        item.put("serviceNetworkId", association.getServiceNetworkId());
        item.put("serviceNetworkName", association.getServiceNetworkName());
        item.put("serviceNetworkArn", association.getServiceNetworkArn());
        putServiceDns(item, association);
        return item;
    }

    private void putServiceDns(ObjectNode node, ServiceNetworkServiceAssociation association) {
        service.serviceByArn(association.getServiceArn()).ifPresent(svc -> {
            putPojo(node, "dnsEntry", svc.getDnsEntry());
            putText(node, "customDomainName", svc.getCustomDomainName());
        });
    }

    private ObjectNode accessLogSubscriptionBody(AccessLogSubscription subscription) {
        ObjectNode item = objectMapper.createObjectNode();
        item.put("id", subscription.getId());
        item.put("arn", subscription.getArn());
        item.put("resourceId", subscription.getResourceId());
        item.put("resourceArn", subscription.getResourceArn());
        item.put("destinationArn", subscription.getDestinationArn());
        putText(item, "serviceNetworkLogType", subscription.getServiceNetworkLogType());
        putTime(item, "createdAt", subscription.getCreatedAt());
        putTime(item, "lastUpdatedAt", subscription.getLastUpdatedAt());
        return item;
    }

    private ObjectNode targetResult(List<Target> targets) {
        ObjectNode response = objectMapper.createObjectNode();
        response.set("successful", objectMapper.valueToTree(targets));
        response.putArray("unsuccessful");
        return response;
    }

    private <T> Response page(List<T> all, Function<T, String> cursorOf, Integer maxResults, String nextToken,
                              Function<T, ObjectNode> render) {
        PaginatedResult<T> result = Pagination.paginate(all, cursorOf, maxResults, nextToken, MAX_PAGE,
                "ValidationException");
        ObjectNode response = objectMapper.createObjectNode();
        ArrayNode items = response.putArray("items");
        result.items().forEach(item -> items.add(render.apply(item)));
        if (result.nextToken() != null) {
            response.put("nextToken", result.nextToken());
        }
        return Response.ok(response).build();
    }

    private JsonNode parse(String body) {
        if (body == null || body.isBlank()) {
            return objectMapper.createObjectNode();
        }
        try {
            return objectMapper.readTree(body);
        } catch (JsonProcessingException e) {
            throw VpcLatticeService.validation("The request body is not valid JSON.");
        }
    }

    private <T> T convert(JsonNode node, Class<T> type) {
        if (node == null || node.isNull()) {
            return null;
        }
        try {
            return objectMapper.treeToValue(node, type);
        } catch (JsonProcessingException e) {
            throw VpcLatticeService.validation("Could not parse " + type.getSimpleName() + ": "
                    + e.getOriginalMessage());
        }
    }

    private List<Target> targets(JsonNode request) {
        JsonNode node = request.get("targets");
        List<Target> targets = new ArrayList<>();
        if (node != null && node.isArray()) {
            node.forEach(item -> targets.add(convert(item, Target.class)));
        }
        return targets;
    }

    private Map<String, String> tags(JsonNode request) {
        JsonNode node = request.get("tags");
        if (node == null || !node.isObject()) {
            return null;
        }
        Map<String, String> tags = new LinkedHashMap<>();
        node.fields().forEachRemaining(entry -> tags.put(entry.getKey(), entry.getValue().asText()));
        return tags;
    }

    private static List<String> strings(JsonNode node) {
        if (node == null || !node.isArray()) {
            return null;
        }
        List<String> values = new ArrayList<>();
        node.forEach(item -> values.add(item.asText()));
        return values;
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asText();
    }

    private static Integer integer(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asInt();
    }

    private static Boolean bool(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asBoolean();
    }

    private void putPojo(ObjectNode node, String field, Object value) {
        if (value != null) {
            node.set(field, objectMapper.valueToTree(value));
        }
    }

    private void putList(ObjectNode node, String field, List<String> values) {
        if (values != null) {
            node.set(field, objectMapper.valueToTree(values));
        }
    }

    private static void putText(ObjectNode node, String field, String value) {
        if (value != null) {
            node.put(field, value);
        }
    }

    private static void putInt(ObjectNode node, String field, Integer value) {
        if (value != null) {
            node.put(field, value);
        }
    }

    private static void putBool(ObjectNode node, String field, Boolean value) {
        if (value != null) {
            node.put(field, value);
        }
    }

    private static void putTime(ObjectNode node, String field, Instant value) {
        if (value != null) {
            node.put(field, DateTimeFormatter.ISO_INSTANT.format(value.truncatedTo(ChronoUnit.MILLIS)));
        }
    }

    private String region(HttpHeaders headers) {
        return regionResolver.resolveRegion(headers);
    }

    private static String decode(String value) {
        return value == null ? null : URLDecoder.decode(value, StandardCharsets.UTF_8);
    }
}
