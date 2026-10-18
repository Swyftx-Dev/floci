package io.github.hectorvent.floci.services.vpclattice.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** A VPC Lattice service. */
@RegisterForReflection
public class LatticeService {

    private String id;
    private String arn;
    private String name;
    private String authType;
    private String customDomainName;
    private String certificateArn;
    private Integer idleTimeoutSeconds;
    private DnsEntry dnsEntry;
    private String status;
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

    public String getAuthType() {
        return authType;
    }

    public void setAuthType(String authType) {
        this.authType = authType;
    }

    public String getCustomDomainName() {
        return customDomainName;
    }

    public void setCustomDomainName(String customDomainName) {
        this.customDomainName = customDomainName;
    }

    public String getCertificateArn() {
        return certificateArn;
    }

    public void setCertificateArn(String certificateArn) {
        this.certificateArn = certificateArn;
    }

    public Integer getIdleTimeoutSeconds() {
        return idleTimeoutSeconds;
    }

    public void setIdleTimeoutSeconds(Integer idleTimeoutSeconds) {
        this.idleTimeoutSeconds = idleTimeoutSeconds;
    }

    public DnsEntry getDnsEntry() {
        return dnsEntry;
    }

    public void setDnsEntry(DnsEntry dnsEntry) {
        this.dnsEntry = dnsEntry;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
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
