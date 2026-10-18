package io.github.hectorvent.floci.core.common.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.Capability;
import com.github.dockerjava.api.model.HostConfig;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.services.lambda.launcher.ImageCacheService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ContainerLifecycleManagerCapabilitiesTest {

    @Mock
    DockerClient dockerClient;

    @Mock
    ImageCacheService imageCacheService;

    @Mock
    ContainerDetector containerDetector;

    @Mock
    PortAllocator portAllocator;

    @Mock
    EmulatorConfig config;

    @Mock
    EmulatorConfig.DockerConfig dockerConfig;

    @Mock
    EmulatorConfig.TlsConfig tlsConfig;

    @BeforeEach
    void setUp() {
        lenient().when(config.docker()).thenReturn(dockerConfig);
        lenient().when(config.tls()).thenReturn(tlsConfig);
        lenient().when(dockerConfig.resourceNamespace()).thenReturn(Optional.empty());
        lenient().when(imageCacheService.ensureImageExists(any()))
                .thenAnswer(invocation -> invocation.getArgument(0));
    }

    @Test
    void requestedCapabilitiesReachHostConfig() {
        CreateContainerCmd createCmd = stubCreateContainer();

        manager().create(specWithCapabilities(List.of("NET_ADMIN", "CAP_SYS_PTRACE"), List.of("mknod")));

        HostConfig hostConfig = capturedHostConfig(createCmd);
        assertArrayEquals(new Capability[] {Capability.NET_ADMIN, Capability.SYS_PTRACE}, hostConfig.getCapAdd());
        assertArrayEquals(new Capability[] {Capability.MKNOD}, hostConfig.getCapDrop());
    }

    @Test
    void anUnknownCapabilityIsSkipped() {
        CreateContainerCmd createCmd = stubCreateContainer();

        manager().create(specWithCapabilities(List.of("NOT_A_CAPABILITY", "NET_RAW"), List.of()));

        assertArrayEquals(new Capability[] {Capability.NET_RAW}, capturedHostConfig(createCmd).getCapAdd());
    }

    @Test
    void containerThatAskedForNoCapabilityGetsNone() {
        CreateContainerCmd createCmd = stubCreateContainer();

        manager().create(new ContainerSpec("busybox:stable"));

        assertNull(capturedHostConfig(createCmd).getCapAdd());
    }

    private ContainerLifecycleManager manager() {
        return new ContainerLifecycleManager(
                dockerClient, imageCacheService, containerDetector, portAllocator, config);
    }

    private static ContainerSpec specWithCapabilities(List<String> add, List<String> drop) {
        return new ContainerSpec(
                "busybox:stable", null, List.of(), null, null, null, Map.of(), List.of(), List.of(), null,
                List.of(), List.of(), List.of(), List.of(), Map.of(), null, false, null, List.of(), null,
                null, List.of(), List.of(), null, null, false, List.of(), Map.of(), false, add, drop);
    }

    private CreateContainerCmd stubCreateContainer() {
        CreateContainerCmd createCmd = mock(CreateContainerCmd.class, RETURNS_SELF);
        when(dockerClient.createContainerCmd("busybox:stable")).thenReturn(createCmd);
        CreateContainerResponse response = mock(CreateContainerResponse.class);
        when(response.getId()).thenReturn("container-id");
        when(createCmd.exec()).thenReturn(response);
        return createCmd;
    }

    private static HostConfig capturedHostConfig(CreateContainerCmd createCmd) {
        ArgumentCaptor<HostConfig> hostConfig = ArgumentCaptor.forClass(HostConfig.class);
        verify(createCmd).withHostConfig(hostConfig.capture());
        return hostConfig.getValue();
    }
}
