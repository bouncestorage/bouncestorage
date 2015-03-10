/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.HashMap;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.policy.MoveEverythingPolicy;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;

import org.apache.commons.configuration.MapConfiguration;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public final class AdminTest {
    private BlobStoreContext bounceContext;
    private BounceBlobStore bounceBlobStore;
    private String containerName;
    private BounceApplication app;

    @Before
    public void setUp() throws Exception {
        containerName = UtilsTest.createRandomContainerName();

        bounceContext = UtilsTest.createTransientBounceBlobStore();

        bounceBlobStore = (BounceBlobStore) bounceContext.getBlobStore();
        bounceBlobStore.createContainerInLocation(null, containerName);


        String config = getClass().getResource("/bounce.yml").toExternalForm();
        app = new BounceApplication(
                new MapConfiguration(new HashMap<>()));
        app.useRandomPorts();
        synchronized (BounceApplication.class) {
            app.run(new String[]{
                    "server", config
            });
        }
        app.useBlobStore(bounceBlobStore);
        BounceService bounceService = app.getBounceService();
        bounceService.setDefaultPolicy(new MoveEverythingPolicy());
    }

    @After
    public void tearDown() throws Exception {
        if (bounceBlobStore != null) {
            bounceBlobStore.deleteContainer(containerName);
        }
        if (bounceContext != null) {
            bounceContext.close();
        }
    }

    // TODO: how to stop DropWizard to re-use port?
    @Ignore
    @Test
    public void testServiceResource() throws Exception {
        ServiceStats stats = new ServiceResource(app).getServiceStats();

        assertThat(stats.getContainerNames()).containsOnly(containerName);
    }

    // TODO: how to stop DropWizard to re-use port?
    @Ignore
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
        Blob blob = bounceBlobStore.blobBuilder(blobName)
                .payload(byteSource)
                .contentLength(byteSource.size())
                .build();
        bounceBlobStore.putBlob(containerName, blob);

        ContainerResource containerResource = new ContainerResource(app);
        ContainerStats stats = containerResource.getContainerStats(containerName);
        assertThat(stats).isEqualToComparingFieldByField(
                new ContainerStats(ImmutableList.of(blobName), 0));

        BounceBlobsResource bounceBlobsResource = new BounceBlobsResource(app);
        bounceBlobsResource.bounceBlobs(containerName, Optional.of(Boolean.TRUE));

        stats = containerResource.getContainerStats(containerName);
        assertThat(stats).isEqualToComparingFieldByField(
                new ContainerStats(ImmutableList.of(blobName), 1));

        Collection<BounceService.BounceTaskStatus> res =
                bounceBlobsResource.status(Optional.absent());
        assertThat(res).hasSize(1);
        BounceService.BounceTaskStatus status = res.iterator().next();
        assertThat(status.getMovedObjectCount()).isEqualTo(1);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);
        assertThat(status.getTotalObjectCount()).isEqualTo(1);
        assertThat(status.future().isDone()).isTrue();
    }
}
