package io.github.hectorvent.floci.testing;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

/** Lets the MSK broker front end open its TLS listeners, which mock mode leaves closed. */
public class RealMskBrokerFrontEndProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of("floci.services.msk.mock", "false");
    }
}
