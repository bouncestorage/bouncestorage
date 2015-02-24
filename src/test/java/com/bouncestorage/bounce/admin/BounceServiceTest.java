/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.BounceService.BounceTaskStatus;
import com.bouncestorage.bounce.admin.policy.BounceEverythingPolicy;
import com.bouncestorage.bounce.admin.policy.LastModifiedTimePolicy;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.configuration.MapConfiguration;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class BounceServiceTest {
    private BlobStoreContext bounceContext;
    private BounceBlobStore blobStore;
    private String containerName;
    private BounceService bounceService;

    @Before
    public void setUp() throws Exception {
        containerName = UtilsTest.createRandomContainerName();

        bounceContext = UtilsTest.createTransientBounceBlobStore();
        blobStore = (BounceBlobStore) bounceContext.getBlobStore();
        blobStore.createContainerInLocation(null, containerName);

        BounceApplication app = new BounceApplication(
                new MapConfiguration(new HashMap<>()));
        app.useRandomPorts();
        app.useBlobStore(blobStore);
        bounceService = app.getBounceService();
    }

    @After
    public void tearDown() throws Exception {
        if (blobStore != null) {
            blobStore.deleteContainer(containerName);
        }
        if (bounceContext != null) {
            bounceContext.close();
        }
    }

    @Test
    public void testBounceNothingPolicy() throws Exception {
        toggleBounceNothing();

        blobStore.putBlob(containerName,
                UtilsTest.makeBlob(blobStore, UtilsTest.createRandomBlobName()));
        BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getTotalObjectCount()).isEqualTo(1);
        assertThat(status.getBouncedObjectCount()).isEqualTo(0);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);
    }

    @Test
    public void testBounceEverythingPolicy() throws Exception {
        toggleBounceEverything();

        blobStore.putBlob(containerName,
                UtilsTest.makeBlob(blobStore, UtilsTest.createRandomBlobName()));
        BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getTotalObjectCount()).isEqualTo(1);
        assertThat(status.getBouncedObjectCount()).isEqualTo(1);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);
    }

    @Test
    public void testLastModifiedTimePolicy() throws Exception {
        BouncePolicy p = lastModifiedTimePolicy(Duration.ofHours(1));
        Blob blob = UtilsTest.makeBlob(blobStore, UtilsTest.createRandomBlobName());
        blob.getMetadata().setLastModified(Date.from(bounceService.getClock().instant()));
        assertThat(p.test(blob.getMetadata())).isFalse();

        advanceServiceClock(Duration.ofHours(2));

        assertThat(p.test(blob.getMetadata())).isTrue();
    }

    @Test
    public void testBounceLastModifiedTimePolicy() throws Exception {
        bounceService.setDefaultPolicy(lastModifiedTimePolicy(Duration.ofHours(1)));

        Blob blob = UtilsTest.makeBlob(blobStore, UtilsTest.createRandomBlobName());
        blobStore.putBlob(containerName, blob);

        BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getTotalObjectCount()).isEqualTo(1);
        assertThat(status.getBouncedObjectCount()).isEqualTo(0);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);

        advanceServiceClock(Duration.ofHours(2));
        status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getTotalObjectCount()).isEqualTo(1);
        assertThat(status.getBouncedObjectCount()).isEqualTo(1);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);
    }

    private void toggleBounceNothing() {
        bounceService.setDefaultPolicy(Optional.empty());
    }

    private void toggleBounceEverything() {
        bounceService.setDefaultPolicy(new BounceEverythingPolicy());
    }

    private BouncePolicy lastModifiedTimePolicy(Duration duration) {
        BouncePolicy p = new LastModifiedTimePolicy();
        Properties properties = new Properties();
        properties.putAll(ImmutableMap.of(
                LastModifiedTimePolicy.DURATION, duration.toString()
        ));
        p.init(bounceService, new MapConfiguration(properties));
        return p;
    }

    public void advanceServiceClock(Duration duration) {
        bounceService.setClock(Clock.offset(bounceService.getClock(), duration));
    }
}
