package io.github.hectorvent.floci.services.ecs;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.hectorvent.floci.core.common.AwsArnUtils;
import io.github.hectorvent.floci.core.common.RequestScopes;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.services.ecs.container.EcsContainerManager;
import io.github.hectorvent.floci.services.ecs.model.Container;
import io.github.hectorvent.floci.services.ecs.model.EcsLoadBalancer;
import io.github.hectorvent.floci.services.ecs.model.EcsRegisteredTargets;
import io.github.hectorvent.floci.services.ecs.model.EcsTask;
import io.github.hectorvent.floci.services.ecs.model.EcsTaskAddress;
import io.github.hectorvent.floci.services.ecs.model.NetworkBinding;
import io.github.hectorvent.floci.services.elbv2.ElbV2Service;
import io.github.hectorvent.floci.services.elbv2.model.TargetDescription;
import io.github.hectorvent.floci.services.elbv2.model.TargetHealth;
import io.github.hectorvent.floci.services.vpclattice.VpcLatticeService;
import io.github.hectorvent.floci.services.vpclattice.model.Target;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Bridges ECS services to target groups: each running task container named by a service's
 * {@code loadBalancers} or {@code vpcLatticeConfigurations} is registered as a target in the
 * ELBv2 or VPC Lattice target group, picked by the group's ARN, and deregistered when the task stops.
 * <p>
 * One-way dependency ECS → ELBv2 and VPC Lattice; their data planes reach the container over plain
 * TCP and never call back into ECS, so there is no cycle.
 */
@ApplicationScoped
public class EcsLoadBalancerRegistrar {

    private static final Logger LOG = Logger.getLogger(EcsLoadBalancerRegistrar.class);

    private final ElbV2Service elbV2Service;
    private final VpcLatticeService vpcLatticeService;
    private final EcsContainerManager containerManager;
    // taskArn → the targets registered for it, persisted so they can be released after a restart
    private final AccountAwareStorageBackend<EcsRegisteredTargets> ledger;

    @Inject
    public EcsLoadBalancerRegistrar(ElbV2Service elbV2Service, VpcLatticeService vpcLatticeService,
                                    EcsContainerManager containerManager, StorageFactory storageFactory) {
        this.elbV2Service = elbV2Service;
        this.vpcLatticeService = vpcLatticeService;
        this.containerManager = containerManager;
        this.ledger = storageFactory.create("ecs", "ecs-registered-targets.json",
                new TypeReference<Map<String, EcsRegisteredTargets>>() {});
    }

    /**
     * Registers the task's load-balanced containers as ELBv2 targets. The targets are recorded
     * before they are registered, so a process killed in between still leaves a record for the
     * next run to release. A target already in its group is recorded too: its address and port
     * are the task's own container, so it reaches nothing else, and once the task stops it would
     * reach nothing or whichever container Docker hands the address to next. ECS on AWS likewise
     * deregisters a stopping task's target whoever registered it.
     */
    public void registerTask(EcsTask task, List<EcsLoadBalancer> bindings, String region) {
        List<EcsRegisteredTargets.Target> planned = new ArrayList<>();
        forEachTarget(task, bindings, (tgArn, td) ->
                planned.add(new EcsRegisteredTargets.Target(tgArn, td.getId(), td.getPort())));
        if (planned.isEmpty()) {
            return;
        }
        String taskArn = task.getTaskArn();
        if (taskArn != null) {
            ledger.put(taskArn, new EcsRegisteredTargets(region, List.copyOf(planned)));
        }
        List<EcsRegisteredTargets.Target> registered = new ArrayList<>();
        for (EcsRegisteredTargets.Target target : planned) {
            try {
                if (isLattice(target.targetGroupArn())) {
                    vpcLatticeService.registerTargets(target.targetGroupArn(), List.of(latticeTarget(target)), region);
                } else {
                    elbV2Service.registerTargets(region, target.targetGroupArn(), List.of(target.toDescription()));
                }
                registered.add(target);
                LOG.infov("Registered ECS task target {0}:{1} into target group {2}",
                        target.id(), target.port(), target.targetGroupArn());
            } catch (Exception e) {
                LOG.warnv("Could not register ECS target into {0}: {1}", target.targetGroupArn(), e.getMessage());
            }
        }
        if (taskArn == null || registered.size() == planned.size()) {
            return;
        }
        if (registered.isEmpty()) {
            ledger.delete(taskArn);
        } else {
            ledger.put(taskArn, new EcsRegisteredTargets(region, List.copyOf(registered)));
        }
    }

    /**
     * Deregisters the targets {@link #registerTask} recorded for the task, and no other: a task
     * with no record registered nothing, as when none of its containers had a reachable binding.
     * Only a task without an ARN, which {@link #registerTask} cannot record, falls back to the
     * targets its containers resolve to now.
     */
    public void deregisterTask(EcsTask task, List<EcsLoadBalancer> bindings, String region) {
        if (task.getTaskArn() == null) {
            forEachTarget(task, bindings, (tgArn, td) -> deregister(region, tgArn, td));
            return;
        }
        Optional<EcsRegisteredTargets> recorded = ledger.get(task.getTaskArn());
        if (recorded.isPresent()) {
            deregister(recorded.get());
            ledger.delete(task.getTaskArn());
        }
    }

    /**
     * Deregisters every target ECS recorded for a task, in every account. Only for startup, when
     * ECS holds no task at all: task state is memory-only, so each recorded target belongs to a
     * task of a previous run whose container is gone or is being removed, and Docker is free to
     * hand its address to an unrelated container. Targets ECS did not register are never touched.
     */
    public void releaseRecordedTargets() {
        for (AccountAwareStorageBackend.AccountEntry<EcsRegisteredTargets> entry
                : ledger.scanAllAccountEntries(key -> true)) {
            RequestScopes.runAs(entry.accountId(), () -> deregister(entry.value()));
            ledger.deleteForAccount(entry.accountId(), entry.key());
            LOG.infov("Released the load balancer targets of ECS task {0} left by a previous run", entry.key());
        }
    }

    /**
     * Deregisters, from the given target groups, each target at one of a starting task's addresses
     * and ports that no ECS task recorded. The address was free until this task took it, so such a target
     * was left by a task that is gone, as one registered by a Floci version that kept no record,
     * and would now send the group's traffic to this task, which belongs to some other service.
     * A target a live task recorded stays, whichever group it is in.
     */
    public void evictUnrecordedTargets(EcsTask task, Collection<String> targetGroupArns,
                                       Collection<EcsTaskAddress> addresses, String region) {
        if (targetGroupArns.isEmpty() || addresses.isEmpty()) {
            return;
        }
        Set<EcsRegisteredTargets.Target> recorded = new HashSet<>();
        for (EcsRegisteredTargets entry : ledger.scan(key -> true)) {
            if (entry.targets() != null) {
                recorded.addAll(entry.targets());
            }
        }
        for (String tgArn : targetGroupArns) {
            List<TargetDescription> registeredTargets;
            try {
                registeredTargets = registeredTargets(region, tgArn);
            } catch (Exception e) {
                // A target group deleted while a service still names it: nothing in it to evict.
                LOG.debugv("Could not list the targets of {0}: {1}", tgArn, e.getMessage());
                continue;
            }
            for (TargetDescription td : registeredTargets) {
                boolean atTaskAddress = addresses.stream().anyMatch(a -> a.matches(td.getId(), td.getPort()));
                if (!atTaskAddress
                        || recorded.contains(new EcsRegisteredTargets.Target(tgArn, td.getId(), td.getPort()))) {
                    continue;
                }
                try {
                    deregisterTargets(region, tgArn, td);
                    LOG.warnv("Deregistered stale target {0}:{1} from target group {2}: no ECS task registered it,"
                            + " and its address now belongs to ECS task {3}", td.getId(), td.getPort(), tgArn,
                            task.getTaskArn());
                } catch (Exception e) {
                    LOG.warnv("Could not deregister stale target {0}:{1} from {2}: {3}", td.getId(), td.getPort(),
                            tgArn, e.getMessage());
                }
            }
        }
    }

    private void deregister(EcsRegisteredTargets recorded) {
        if (recorded.targets() == null) {
            return;
        }
        for (EcsRegisteredTargets.Target target : recorded.targets()) {
            deregister(recorded.region(), target.targetGroupArn(), target.toDescription());
        }
    }

    private void deregister(String region, String tgArn, TargetDescription td) {
        try {
            deregisterTargets(region, tgArn, td);
            LOG.infov("Deregistered ECS task target {0}:{1} from target group {2}",
                    td.getId(), td.getPort(), tgArn);
        } catch (Exception e) {
            LOG.warnv("Could not deregister ECS target from {0}: {1}", tgArn, e.getMessage());
        }
    }

    private List<TargetDescription> registeredTargets(String region, String tgArn) {
        if (isLattice(tgArn)) {
            return vpcLatticeService.listTargets(tgArn, null, region).stream()
                    .map(t -> new EcsRegisteredTargets.Target(tgArn, t.id(), t.port()).toDescription())
                    .toList();
        }
        return elbV2Service.describeTargetHealth(region, tgArn, null).stream().map(TargetHealth::getTarget).toList();
    }

    private void deregisterTargets(String region, String tgArn, TargetDescription td) {
        if (isLattice(tgArn)) {
            vpcLatticeService.deregisterTargets(tgArn,
                    List.of(latticeTarget(new EcsRegisteredTargets.Target(tgArn, td.getId(), td.getPort()))), region);
        } else {
            elbV2Service.deregisterTargets(region, tgArn, List.of(td));
        }
    }

    private static boolean isLattice(String targetGroupArn) {
        return AwsArnUtils.isArnFor(targetGroupArn, "vpc-lattice");
    }

    private static Target latticeTarget(EcsRegisteredTargets.Target registered) {
        Target target = new Target();
        target.setId(registered.id());
        target.setPort(registered.port());
        return target;
    }

    private void forEachTarget(EcsTask task, List<EcsLoadBalancer> bindings,
                               BiConsumer<String, TargetDescription> action) {
        if (bindings == null || bindings.isEmpty()) {
            return;
        }
        if (task.getContainers() == null || task.getContainers().isEmpty()) {
            return;
        }
        for (EcsLoadBalancer lb : bindings) {
            if (lb.getTargetGroupArn() == null || lb.getTargetGroupArn().isBlank()) {
                continue;
            }
            Container container = task.getContainers().stream()
                    .filter(c -> lb.getContainerName() == null
                            || lb.getContainerName().equals(c.getName()))
                    .findFirst()
                    .orElse(null);
            if (container == null || container.getNetworkBindings() == null) {
                continue;
            }
            NetworkBinding binding = container.getNetworkBindings().stream()
                    .filter(b -> lb.getContainerPort() == null
                            || lb.getContainerPort() == b.containerPort())
                    .findFirst()
                    .orElse(null);
            if (binding == null) {
                continue;
            }
            TargetDescription td = new TargetDescription();
            td.setId(containerManager.resolveContainerHost(container));
            td.setPort(EcsTaskAddress.isLoopback(td.getId()) ? binding.hostPort() : binding.containerPort());
            action.accept(lb.getTargetGroupArn(), td);
        }
    }
}
