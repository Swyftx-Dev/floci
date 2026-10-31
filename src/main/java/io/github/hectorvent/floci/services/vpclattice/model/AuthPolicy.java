package io.github.hectorvent.floci.services.vpclattice.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.time.Instant;

/** The auth policy attached to a service or service network. Stored only, never evaluated. */
@RegisterForReflection
public class AuthPolicy {

    private String resourceArn;
    private String policy;
    private Instant createdAt;
    private Instant lastUpdatedAt;

    public String getResourceArn() {
        return resourceArn;
    }

    public void setResourceArn(String resourceArn) {
        this.resourceArn = resourceArn;
    }

    public String getPolicy() {
        return policy;
    }

    public void setPolicy(String policy) {
        this.policy = policy;
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
}
