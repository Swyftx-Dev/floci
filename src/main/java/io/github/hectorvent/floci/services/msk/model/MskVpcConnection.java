package io.github.hectorvent.floci.services.msk.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** A multi-VPC private connection from a client VPC to a cluster, possibly in another account. */
@RegisterForReflection
public class MskVpcConnection {

    private String vpcConnectionArn;
    private String targetClusterArn;
    private String authentication;
    private String vpcId;
    private List<String> clientSubnets;
    private List<String> securityGroups;
    private Instant creationTime;
    private String state;
    private String accountId;
    private String region;
    private Map<String, String> tags;

    public String getVpcConnectionArn() { return vpcConnectionArn; }

    public void setVpcConnectionArn(String vpcConnectionArn) { this.vpcConnectionArn = vpcConnectionArn; }

    public String getTargetClusterArn() { return targetClusterArn; }

    public void setTargetClusterArn(String targetClusterArn) { this.targetClusterArn = targetClusterArn; }

    public String getAuthentication() { return authentication; }

    public void setAuthentication(String authentication) { this.authentication = authentication; }

    public String getVpcId() { return vpcId; }

    public void setVpcId(String vpcId) { this.vpcId = vpcId; }

    public List<String> getClientSubnets() { return clientSubnets; }

    public void setClientSubnets(List<String> clientSubnets) { this.clientSubnets = clientSubnets; }

    public List<String> getSecurityGroups() { return securityGroups; }

    public void setSecurityGroups(List<String> securityGroups) { this.securityGroups = securityGroups; }

    public Instant getCreationTime() { return creationTime; }

    public void setCreationTime(Instant creationTime) { this.creationTime = creationTime; }

    public String getState() { return state; }

    public void setState(String state) { this.state = state; }

    public String getAccountId() { return accountId; }

    public void setAccountId(String accountId) { this.accountId = accountId; }

    public String getRegion() { return region; }

    public void setRegion(String region) { this.region = region; }

    public Map<String, String> getTags() { return tags; }

    public void setTags(Map<String, String> tags) { this.tags = tags; }
}
