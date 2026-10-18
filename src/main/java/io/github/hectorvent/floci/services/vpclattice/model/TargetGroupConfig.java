package io.github.hectorvent.floci.services.vpclattice.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.runtime.annotations.RegisterForReflection;

/** A target group's configuration, in the wire shape. */
@RegisterForReflection
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TargetGroupConfig {

    private Integer port;
    private String protocol;
    private String protocolVersion;
    private String ipAddressType;
    private String vpcIdentifier;
    private HealthCheckConfig healthCheck;
    private String lambdaEventStructureVersion;

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
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

    public String getIpAddressType() {
        return ipAddressType;
    }

    public void setIpAddressType(String ipAddressType) {
        this.ipAddressType = ipAddressType;
    }

    public String getVpcIdentifier() {
        return vpcIdentifier;
    }

    public void setVpcIdentifier(String vpcIdentifier) {
        this.vpcIdentifier = vpcIdentifier;
    }

    public HealthCheckConfig getHealthCheck() {
        return healthCheck;
    }

    public void setHealthCheck(HealthCheckConfig healthCheck) {
        this.healthCheck = healthCheck;
    }

    public String getLambdaEventStructureVersion() {
        return lambdaEventStructureVersion;
    }

    public void setLambdaEventStructureVersion(String lambdaEventStructureVersion) {
        this.lambdaEventStructureVersion = lambdaEventStructureVersion;
    }
}
