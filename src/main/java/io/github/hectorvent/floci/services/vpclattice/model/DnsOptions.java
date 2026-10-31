package io.github.hectorvent.floci.services.vpclattice.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.List;

/** Which custom domain names a VPC association resolves privately. */
@RegisterForReflection
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DnsOptions {

    private String privateDnsPreference;
    private List<String> privateDnsSpecifiedDomains;

    public String getPrivateDnsPreference() {
        return privateDnsPreference;
    }

    public void setPrivateDnsPreference(String privateDnsPreference) {
        this.privateDnsPreference = privateDnsPreference;
    }

    public List<String> getPrivateDnsSpecifiedDomains() {
        return privateDnsSpecifiedDomains;
    }

    public void setPrivateDnsSpecifiedDomains(List<String> privateDnsSpecifiedDomains) {
        this.privateDnsSpecifiedDomains = privateDnsSpecifiedDomains;
    }
}
