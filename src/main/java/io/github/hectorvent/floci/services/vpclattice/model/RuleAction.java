package io.github.hectorvent.floci.services.vpclattice.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.runtime.annotations.RegisterForReflection;

/** A listener's action: exactly one of forward or fixedResponse. */
@RegisterForReflection
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RuleAction {

    private ForwardAction forward;
    private FixedResponseAction fixedResponse;

    public ForwardAction getForward() {
        return forward;
    }

    public void setForward(ForwardAction forward) {
        this.forward = forward;
    }

    public FixedResponseAction getFixedResponse() {
        return fixedResponse;
    }

    public void setFixedResponse(FixedResponseAction fixedResponse) {
        this.fixedResponse = fixedResponse;
    }
}
