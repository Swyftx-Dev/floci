package io.github.hectorvent.floci.services.vpclattice.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.runtime.annotations.RegisterForReflection;

/** A target group's health check, in the wire shape. */
@RegisterForReflection
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class HealthCheckConfig {

    private Boolean enabled;
    private String protocol;
    private String protocolVersion;
    private Integer port;
    private String path;
    private Integer healthCheckIntervalSeconds;
    private Integer healthCheckTimeoutSeconds;
    private Integer healthyThresholdCount;
    private Integer unhealthyThresholdCount;
    private Matcher matcher;

    public Boolean getEnabled() {
        return enabled;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(String protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Integer getHealthCheckIntervalSeconds() {
        return healthCheckIntervalSeconds;
    }

    public void setHealthCheckIntervalSeconds(Integer healthCheckIntervalSeconds) {
        this.healthCheckIntervalSeconds = healthCheckIntervalSeconds;
    }

    public Integer getHealthCheckTimeoutSeconds() {
        return healthCheckTimeoutSeconds;
    }

    public void setHealthCheckTimeoutSeconds(Integer healthCheckTimeoutSeconds) {
        this.healthCheckTimeoutSeconds = healthCheckTimeoutSeconds;
    }

    public Integer getHealthyThresholdCount() {
        return healthyThresholdCount;
    }

    public void setHealthyThresholdCount(Integer healthyThresholdCount) {
        this.healthyThresholdCount = healthyThresholdCount;
    }

    public Integer getUnhealthyThresholdCount() {
        return unhealthyThresholdCount;
    }

    public void setUnhealthyThresholdCount(Integer unhealthyThresholdCount) {
        this.unhealthyThresholdCount = unhealthyThresholdCount;
    }

    public Matcher getMatcher() {
        return matcher;
    }

    public void setMatcher(Matcher matcher) {
        this.matcher = matcher;
    }
}
