package io.github.hectorvent.floci.services.vpclattice.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.List;

/** Forwards requests to weighted target groups. */
@RegisterForReflection
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ForwardAction {

    private List<WeightedTargetGroup> targetGroups;

    public List<WeightedTargetGroup> getTargetGroups() {
        return targetGroups;
    }

    public void setTargetGroups(List<WeightedTargetGroup> targetGroups) {
        this.targetGroups = targetGroups;
    }
}
