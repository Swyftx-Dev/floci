package io.github.hectorvent.floci.services.vpclattice;

import io.github.hectorvent.floci.core.common.SsrfProtection;
import io.github.hectorvent.floci.services.elbv2.ElbV2HealthChecker;
import io.github.hectorvent.floci.services.vpclattice.model.HealthCheckConfig;
import io.github.hectorvent.floci.services.vpclattice.model.Target;
import io.github.hectorvent.floci.services.vpclattice.model.TargetGroup;
import io.vertx.core.Vertx;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Active HTTP health checks: one Vert.x timer per target group that has health checks enabled and at
 * least one target, moving each target between {@code INITIAL}, {@code HEALTHY} and
 * {@code UNHEALTHY} after the configured thresholds.
 */
@ApplicationScoped
public class VpcLatticeHealthChecker {

    private static final Logger LOG = Logger.getLogger(VpcLatticeHealthChecker.class);

    public record Health(String status, String reasonCode) {}

    private static final class TargetState {
        private volatile String status = "INITIAL";
        private volatile String reasonCode = "HealthCheckInProgress";
        private int consecutiveSuccesses;
        private int consecutiveFailures;
    }

    private static final class Monitor {
        private final long timerId;
        private final HealthCheckConfig healthCheck;
        private final Map<String, TargetState> states = new ConcurrentHashMap<>();

        private Monitor(long timerId, HealthCheckConfig healthCheck) {
            this.timerId = timerId;
            this.healthCheck = healthCheck;
        }
    }

    private final Vertx vertx;
    private final Map<String, Monitor> monitors = new ConcurrentHashMap<>();
    private final Map<String, TargetGroup> groups = new ConcurrentHashMap<>();

    @Inject
    public VpcLatticeHealthChecker(Vertx vertx) {
        this.vertx = vertx;
    }

    public synchronized void sync(List<TargetGroup> targetGroups) {
        Set<String> live = new HashSet<>();
        for (TargetGroup tg : targetGroups) {
            if (!monitored(tg)) {
                continue;
            }
            live.add(tg.getArn());
            groups.put(tg.getArn(), tg);
            HealthCheckConfig healthCheck = tg.getConfig().getHealthCheck();
            Monitor monitor = monitors.get(tg.getArn());
            if (monitor == null || !sameSchedule(monitor.healthCheck, healthCheck)) {
                if (monitor != null) {
                    vertx.cancelTimer(monitor.timerId);
                }
                long intervalMs = Math.max(1, orDefault(healthCheck.getHealthCheckIntervalSeconds(), 30)) * 1000L;
                Monitor replacement = new Monitor(vertx.setPeriodic(intervalMs, id -> probeAll(tg.getArn())),
                        healthCheck);
                if (monitor != null) {
                    replacement.states.putAll(monitor.states);
                }
                monitor = replacement;
                monitors.put(tg.getArn(), monitor);
            }
            Set<String> keys = new HashSet<>();
            boolean added = false;
            for (Target target : tg.getTargets()) {
                String key = key(target);
                keys.add(key);
                added |= monitor.states.putIfAbsent(key, new TargetState()) == null;
            }
            monitor.states.keySet().retainAll(keys);
            if (added) {
                vertx.setTimer(1, id -> probeAll(tg.getArn()));
            }
        }
        for (String arn : Set.copyOf(monitors.keySet())) {
            if (!live.contains(arn)) {
                vertx.cancelTimer(monitors.remove(arn).timerId);
                groups.remove(arn);
            }
        }
    }

    public Health health(TargetGroup tg, Target target) {
        if (!Boolean.TRUE.equals(healthCheckOf(tg).map(HealthCheckConfig::getEnabled).orElse(null))) {
            return new Health("UNAVAILABLE", "HealthCheckDisabled");
        }
        Monitor monitor = monitors.get(tg.getArn());
        TargetState state = monitor == null ? null : monitor.states.get(key(target));
        return state == null ? new Health("INITIAL", "HealthCheckInProgress")
                : new Health(state.status, state.reasonCode);
    }

    public boolean isRoutable(TargetGroup tg, Target target) {
        return !"UNHEALTHY".equals(health(tg, target).status());
    }

    @PreDestroy
    synchronized void stop() {
        monitors.values().forEach(monitor -> vertx.cancelTimer(monitor.timerId));
        monitors.clear();
        groups.clear();
    }

    private void probeAll(String tgArn) {
        Monitor monitor = monitors.get(tgArn);
        TargetGroup tg = groups.get(tgArn);
        if (monitor == null || tg == null) {
            return;
        }
        HealthCheckConfig healthCheck = monitor.healthCheck;
        for (Target target : tg.getTargets()) {
            TargetState state = monitor.states.get(key(target));
            if (state == null) {
                continue;
            }
            int port = healthCheck.getPort() != null ? healthCheck.getPort()
                    : target.getPort() != null ? target.getPort() : tg.getConfig().getPort();
            String path = healthCheck.getPath() != null ? healthCheck.getPath() : "/";
            int timeoutSeconds = orDefault(healthCheck.getHealthCheckTimeoutSeconds(), 5);
            vertx.executeBlocking(() -> probe(target.getId(), port, path, timeoutSeconds), false)
                    .onSuccess(code -> record(state, healthCheck, ElbV2HealthChecker.matchesStatusCode(code,
                            healthCheck.getMatcher() != null && healthCheck.getMatcher().getHttpCode() != null
                                    ? healthCheck.getMatcher().getHttpCode() : "200"), "HealthCheckFailed"))
                    .onFailure(err -> {
                        LOG.debugv("Lattice health check of {0}:{1} failed: {2}", target.getId(),
                                String.valueOf(port), err.getMessage());
                        record(state, healthCheck, false, "ConnectionFailed");
                    });
        }
    }

    private static void record(TargetState state, HealthCheckConfig healthCheck, boolean success,
                               String failureReason) {
        synchronized (state) {
            if (success) {
                state.consecutiveFailures = 0;
                state.consecutiveSuccesses++;
                if (state.consecutiveSuccesses >= orDefault(healthCheck.getHealthyThresholdCount(), 5)) {
                    state.status = "HEALTHY";
                    state.reasonCode = null;
                }
                return;
            }
            state.consecutiveSuccesses = 0;
            state.consecutiveFailures++;
            if (state.consecutiveFailures >= orDefault(healthCheck.getUnhealthyThresholdCount(), 2)) {
                state.status = "UNHEALTHY";
                state.reasonCode = failureReason;
            }
        }
    }

    private static int probe(String host, int port, String path, int timeoutSeconds) throws IOException {
        InetAddress[] addresses = SsrfProtection.rejectMetadataAddresses(InetAddress.getAllByName(host), host);
        String address = addresses[0].getHostAddress();
        String authority = address.indexOf(':') >= 0 ? "[" + address + "]" : address;
        HttpURLConnection connection = (HttpURLConnection) URI.create("http://" + authority + ":" + port
                + (path.startsWith("/") ? path : "/" + path)).toURL().openConnection();
        connection.setConnectTimeout(timeoutSeconds * 1000);
        connection.setReadTimeout(timeoutSeconds * 1000);
        connection.setInstanceFollowRedirects(false);
        try {
            return connection.getResponseCode();
        } finally {
            connection.disconnect();
        }
    }

    private static boolean monitored(TargetGroup tg) {
        return healthCheckOf(tg).map(HealthCheckConfig::getEnabled).filter(Boolean.TRUE::equals).isPresent()
                && !tg.getTargets().isEmpty();
    }

    private static Optional<HealthCheckConfig> healthCheckOf(TargetGroup tg) {
        return Optional.ofNullable(tg.getConfig()).map(c -> c.getHealthCheck());
    }

    private static boolean sameSchedule(HealthCheckConfig a, HealthCheckConfig b) {
        return Objects.equals(a.getHealthCheckIntervalSeconds(), b.getHealthCheckIntervalSeconds())
                && Objects.equals(a.getPath(), b.getPath()) && Objects.equals(a.getPort(), b.getPort())
                && Objects.equals(a.getMatcher() == null ? null : a.getMatcher().getHttpCode(),
                        b.getMatcher() == null ? null : b.getMatcher().getHttpCode())
                && Objects.equals(a.getHealthyThresholdCount(), b.getHealthyThresholdCount())
                && Objects.equals(a.getUnhealthyThresholdCount(), b.getUnhealthyThresholdCount())
                && Objects.equals(a.getHealthCheckTimeoutSeconds(), b.getHealthCheckTimeoutSeconds());
    }

    private static int orDefault(Integer value, int fallback) {
        return value != null ? value : fallback;
    }

    private static String key(Target target) {
        return target.getId() + ":" + target.getPort();
    }
}
