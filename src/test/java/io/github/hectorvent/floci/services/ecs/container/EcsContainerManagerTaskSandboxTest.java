package io.github.hectorvent.floci.services.ecs.container;

import com.github.dockerjava.api.DockerClient;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.common.docker.ContainerBuilder;
import io.github.hectorvent.floci.core.common.docker.ContainerDetector;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager.ContainerInfo;
import io.github.hectorvent.floci.core.common.docker.ContainerLogStreamer;
import io.github.hectorvent.floci.core.common.docker.LaunchedContainerAwsEnv;
import io.github.hectorvent.floci.services.ec2.Ec2Service;
import io.github.hectorvent.floci.services.ec2.SecurityGroupFirewallManager;
import io.github.hectorvent.floci.services.ecr.registry.EcrRegistryManager;
import io.github.hectorvent.floci.services.ecs.model.ContainerDefinition;
import io.github.hectorvent.floci.services.ecs.model.EcsTask;
import io.github.hectorvent.floci.services.ecs.model.NetworkMode;
import io.github.hectorvent.floci.services.ecs.model.TaskDefinition;
import io.github.hectorvent.floci.services.lambda.launcher.ImageCacheService.LaunchImage;
import io.github.hectorvent.floci.services.s3.S3Service;
import io.github.hectorvent.floci.services.secretsmanager.SecretsManagerService;
import io.github.hectorvent.floci.services.ssm.SsmService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * An awsvpc task on a Docker network runs its containers in one pause container's namespace. The
 * builder and lifecycle manager are mocked, so this checks what would reach Docker without a daemon.
 */
class EcsContainerManagerTaskSandboxTest {

    private ContainerBuilder.Builder builder;
    private ContainerLifecycleManager lifecycleManager;
    private EcsContainerManager manager;

    @BeforeEach
    void setUp() {
        builder = mock(ContainerBuilder.Builder.class, RETURNS_SELF);
        ContainerBuilder containerBuilder = mock(ContainerBuilder.class);
        when(containerBuilder.newContainer(any())).thenReturn(builder);
        when(containerBuilder.resolveImage(anyString())).thenAnswer(invocation -> invocation.getArgument(0));
        when(containerBuilder.resolveDockerNetwork(any())).thenReturn(Optional.of("floci-net"));

        lifecycleManager = mock(ContainerLifecycleManager.class);
        when(lifecycleManager.getDockerClient()).thenReturn(mock(DockerClient.class, RETURNS_DEEP_STUBS));
        when(lifecycleManager.resolveImageForLaunch(any(), any(), any()))
                .thenAnswer(invocation -> new LaunchImage(invocation.getArgument(0), null));
        when(lifecycleManager.createAndStart(any())).thenReturn(new ContainerInfo("sandbox-id", Map.of()));
        when(lifecycleManager.createAndStartEphemeral(any(), any())).thenReturn(
                new ContainerInfo("app-id", Map.of()),
                new ContainerInfo("sidecar-id", Map.of()));

        EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
        when(config.services().ecs().dockerNetwork()).thenReturn(Optional.of("floci-net"));
        LaunchedContainerAwsEnv awsEnv = mock(LaunchedContainerAwsEnv.class);
        when(awsEnv.sdkBaselineEnv(any(), any())).thenReturn(List.of());
        EcrRegistryManager ecrRegistryManager = mock(EcrRegistryManager.class);
        when(ecrRegistryManager.rewriteImageUri(anyString())).thenAnswer(invocation -> invocation.getArgument(0));

        manager = new EcsContainerManager(containerBuilder, lifecycleManager, mock(ContainerLogStreamer.class),
                mock(ContainerDetector.class), config, mock(RegionResolver.class), awsEnv, mock(SsmService.class),
                mock(SecretsManagerService.class), mock(S3Service.class), ecrRegistryManager,
                mock(HostVolumePolicy.class), mock(Ec2Service.class), mock(SecurityGroupFirewallManager.class),
                mock(EcsTaskRoleCredentials.class), mock(EcsCredentialsProxy.class),
                mock(EcsTaskLinkLocalAddresses.class), null, null);
    }

    @Test
    void awsvpcContainersJoinThePauseContainersNamespace() {
        TaskDefinition taskDef = new TaskDefinition();
        taskDef.setFamily("web");
        taskDef.setNetworkMode(NetworkMode.awsvpc);
        taskDef.setContainerDefinitions(List.of(container("app"), container("sidecar")));
        EcsTask task = new EcsTask();
        task.setTaskArn("arn:aws:ecs:us-east-1:000000000000:task/cluster/abc123");

        manager.startTask(task, taskDef, List.of(), "us-east-1");

        ArgumentCaptor<String> names = ArgumentCaptor.forClass(String.class);
        verify(builder, times(3)).withName(names.capture());
        assertTrue(names.getAllValues().getFirst().endsWith("ecs-abc123-internal-ecs-pause"),
                names.getAllValues().getFirst());
        verify(builder).withCmd(List.of("sleep", "2147483647"));
        verify(builder, times(2)).withNetworkMode("container:sandbox-id");
        verify(builder, never()).withLinkLocalIp(anyString());
        assertEquals(3, names.getAllValues().size());
    }

    private static ContainerDefinition container(String name) {
        ContainerDefinition def = new ContainerDefinition();
        def.setName(name);
        def.setImage(name + ":latest");
        def.setEssential(true);
        return def;
    }
}
