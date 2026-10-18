package io.github.hectorvent.floci.services.vpclattice.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** A VPC joined to a service network. */
@RegisterForReflection
public class ServiceNetworkVpcAssociation {

    private String id;
    private String arn;
    private String status;
    private String createdBy;
    private String serviceNetworkId;
    private String serviceNetworkName;
    private String serviceNetworkArn;
    private String vpcId;
    private List<String> securityGroupIds;
    private Boolean privateDnsEnabled;
    private DnsOptions dnsOptions;
    private String region;
    private String accountId;
    private Instant createdAt;
    private Instant lastUpdatedAt;
    private Map<String, String> tags = new LinkedHashMap<>();

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getArn() {
        return arn;
    }

    public void setArn(String arn) {
        this.arn = arn;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getServiceNetworkId() {
        return serviceNetworkId;
    }

    public void setServiceNetworkId(String serviceNetworkId) {
        this.serviceNetworkId = serviceNetworkId;
    }

    public String getServiceNetworkName() {
        return serviceNetworkName;
    }

    public void setServiceNetworkName(String serviceNetworkName) {
        this.serviceNetworkName = serviceNetworkName;
    }

    public String getServiceNetworkArn() {
        return serviceNetworkArn;
    }

    public void setServiceNetworkArn(String serviceNetworkArn) {
        this.serviceNetworkArn = serviceNetworkArn;
    }

    public String getVpcId() {
        return vpcId;
    }

    public void setVpcId(String vpcId) {
        this.vpcId = vpcId;
    }

    public List<String> getSecurityGroupIds() {
        return securityGroupIds;
    }

    public void setSecurityGroupIds(List<String> securityGroupIds) {
        this.securityGroupIds = securityGroupIds;
    }

    public Boolean getPrivateDnsEnabled() {
        return privateDnsEnabled;
    }

    public void setPrivateDnsEnabled(Boolean privateDnsEnabled) {
        this.privateDnsEnabled = privateDnsEnabled;
    }

    public DnsOptions getDnsOptions() {
        return dnsOptions;
    }

    public void setDnsOptions(DnsOptions dnsOptions) {
        this.dnsOptions = dnsOptions;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getLastUpdatedAt() {
        return lastUpdatedAt;
    }

    public void setLastUpdatedAt(Instant lastUpdatedAt) {
        this.lastUpdatedAt = lastUpdatedAt;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }
}
