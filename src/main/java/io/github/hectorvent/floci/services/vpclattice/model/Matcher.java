package io.github.hectorvent.floci.services.vpclattice.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.runtime.annotations.RegisterForReflection;

/** The HTTP codes a health check accepts, as {@code 200}, {@code 200,202} or {@code 200-299}. */
@RegisterForReflection
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Matcher {

    private String httpCode;

    public String getHttpCode() {
        return httpCode;
    }

    public void setHttpCode(String httpCode) {
        this.httpCode = httpCode;
    }
}
