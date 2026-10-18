package io.github.hectorvent.floci.services.vpclattice.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@RegisterForReflection
public class Listener {

    private String id;
    private String arn;
    private String name;
    private String protocol;
    private Integer port;
    private String serviceId;
    private String serviceArn;
    private RuleAction defaultAction;
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getServiceId() {
        return serviceId;
    }

    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }

    public String getServiceArn() {
        return serviceArn;
    }

    public void setServiceArn(String serviceArn) {
        this.serviceArn = serviceArn;
    }

    public RuleAction getDefaultAction() {
        return defaultAction;
    }

    public void setDefaultAction(RuleAction defaultAction) {
        this.defaultAction = defaultAction;
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
