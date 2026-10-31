package io.github.hectorvent.floci.services.vpclattice.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.runtime.annotations.RegisterForReflection;

/** A target group a forward action sends a share of requests to. */
@RegisterForReflection
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class WeightedTargetGroup {

    private String targetGroupIdentifier;
    private Integer weight;

    public String getTargetGroupIdentifier() {
        return targetGroupIdentifier;
    }

    public void setTargetGroupIdentifier(String targetGroupIdentifier) {
        this.targetGroupIdentifier = targetGroupIdentifier;
    }

    public Integer getWeight() {
        return weight;
    }

    public void setWeight(Integer weight) {
        this.weight = weight;
    }
}
