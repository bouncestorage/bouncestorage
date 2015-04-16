/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static com.bouncestorage.bounce.UtilsTest.assertStatus;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.Properties;

import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.policy.WriteBackPolicy;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;

import org.jclouds.blobstore.domain.Blob;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
public final class AdminTest {
    private BouncePolicy policy;
    private String containerName;
    private BounceApplication app;

    @Before
    public void setUp() throws Exception {
        containerName = UtilsTest.createRandomContainerName();

        Properties properties = new Properties();
        try (InputStream is = AdminTest.class.getResourceAsStream("/bounce.properties")) {
            properties.load(is);
        }

        String config = getClass().getResource("/bounce.yml").toExternalForm();
        synchronized (BounceApplication.class) {
            app = new BounceApplication();
            app.getConfiguration().setAll(properties);
            app.useRandomPorts();
            app.registerConfigurationListener();
            app.pauseBackgroundTasks();

            app.run(new String[]{
                    "server", config
            });
        }

        UtilsTest.createTestProvidersConfig(app.getConfiguration());
        UtilsTest.switchPolicyforContainer(app, containerName, WriteBackPolicy.class,
                ImmutableMap.of(WriteBackPolicy.COPY_DELAY, Duration.ofSeconds(-1).toString(),
                        WriteBackPolicy.EVICT_DELAY, Duration.ofSeconds(0).toString()));
        policy = (BouncePolicy) app.getBlobStore(containerName);
        policy.createContainerInLocation(null, containerName);
    }

    @After
    public void tearDown() throws Exception {
        if (policy != null) {
            policy.deleteContainer(containerName);
        }
    }

    @Test
    public void testServiceResource() throws Exception {
        ServiceStats stats = new ServiceResource(app).getServiceStats();

        assertThat(stats.getContainerNames()).contains(containerName);
    }

    @Test
    public void testContainerResource() throws Exception {
        ContainerStats stats = new ContainerResource(app)
                .getContainerStats(containerName);
        assertThat(stats).isEqualToComparingFieldByField(new ContainerStats(ImmutableList.of(), 0));
    }

    @Test
    public void testBounceBlobsResource() throws Exception {
        String blobName = "blob";
        ByteSource byteSource = ByteSource.wrap(new byte[0]);
        Blob blob = policy.blobBuilder(blobName)
                .payload(byteSource)
                .contentLength(byteSource.size())
                .build();
        policy.putBlob(containerName, blob);

        ContainerResource containerResource = new ContainerResource(app);
        ContainerStats stats = containerResource.getContainerStats(containerName);
        assertThat(stats).isEqualToComparingFieldByField(
                new ContainerStats(ImmutableList.of(blobName), 0));

        BounceBlobsResource bounceBlobsResource = new BounceBlobsResource(app);
        bounceBlobsResource.bounceBlobs(new BounceBlobsResource.BounceServiceRequest(Optional.of(containerName),
                Optional.of(true), Optional.absent()));

        stats = containerResource.getContainerStats(containerName);
        assertThat(stats).isEqualToComparingFieldByField(
                new ContainerStats(ImmutableList.of(blobName), 1));

        Collection<BounceService.BounceTaskStatus> res = bounceBlobsResource.status();
        assertThat(res).hasSize(1);
        BounceService.BounceTaskStatus status = res.iterator().next();
        assertStatus(status, status::getMovedObjectCount).isEqualTo(1);
        assertStatus(status, status::getErrorObjectCount).isEqualTo(0);
        assertStatus(status, status::getTotalObjectCount).isEqualTo(1);
        assertThat(status.future().isDone()).isTrue();
    }

    @Test
    public void testBounceAbort() throws Exception {
        String blobName = "blob";
        ByteSource byteSource = ByteSource.wrap(new byte[0]);
        Blob blob = policy.blobBuilder(blobName)
                .payload(byteSource)
                .contentLength(byteSource.size())
                .build();
        policy.putBlob(containerName, blob);

        BounceBlobsResource bounceBlobsResource = new BounceBlobsResource(app);
        BounceService.BounceTaskStatus status = bounceBlobsResource.bounceBlobs(new BounceBlobsResource
                .BounceServiceRequest(Optional.of(containerName), Optional.absent(), Optional.absent()));
        assertThat(status.aborted).isFalse();

        status = bounceBlobsResource.bounceBlobs(new BounceBlobsResource.BounceServiceRequest(
                Optional.of(containerName), Optional.absent(), Optional.of(true)));
        assertThat(status.aborted).isTrue();
    }
}
