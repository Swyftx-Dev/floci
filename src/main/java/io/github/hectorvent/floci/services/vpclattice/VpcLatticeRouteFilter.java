package io.github.hectorvent.floci.services.vpclattice;

import io.github.hectorvent.floci.core.common.ResolvedServiceCatalog;
import io.github.hectorvent.floci.core.common.SignedServiceRoute;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.ext.Provider;

/** Lattice paths such as {@code /services} and {@code /tags/{arn}} are otherwise claimed by S3 and other services. */
@Provider
@PreMatching
public class VpcLatticeRouteFilter implements ContainerRequestFilter {

    static final String INTERNAL_PREFIX = "/_vpclattice";

    private final SignedServiceRoute route;

    @Inject
    public VpcLatticeRouteFilter(ResolvedServiceCatalog catalog) {
        this.route = new SignedServiceRoute(catalog, "vpc-lattice", INTERNAL_PREFIX, path -> !path.equals("/"));
    }

    @Override
    public void filter(ContainerRequestContext ctx) {
        route.apply(ctx);
    }
}
