package io.github.hectorvent.floci.testing;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

/** Runs the real VPC Lattice and ELBv2 data planes, which share their listener ports. */
public class RealVpcLatticeDataPlaneProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of("floci.services.vpclattice.mock", "false", "floci.services.elbv2.mock", "false");
    }
}
