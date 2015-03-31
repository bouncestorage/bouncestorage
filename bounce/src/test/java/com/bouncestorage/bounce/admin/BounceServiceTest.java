/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.BounceService.BounceTaskStatus;
import com.bouncestorage.bounce.admin.policy.BounceNothingPolicy;
import com.bouncestorage.bounce.admin.policy.CopyPolicy;
import com.bouncestorage.bounce.admin.policy.LastModifiedTimePolicy;
import com.bouncestorage.bounce.admin.policy.MoveEverythingPolicy;
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
    BounceApplication app;

    @Before
    public void setUp() throws Exception {
        containerName = UtilsTest.createRandomContainerName();

        bounceContext = UtilsTest.createTestBounceBlobStore();
        blobStore = (BounceBlobStore) bounceContext.getBlobStore();
        blobStore.createContainerInLocation(null, containerName);

        synchronized (BounceApplication.class) {
            app = new BounceApplication(new MapConfiguration(new HashMap<>()));
        }
        app.useRandomPorts();
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
        assertThat(status.getMovedObjectCount()).isEqualTo(0);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);
    }

    @Test
    public void testMoveEverythingPolicy() throws Exception {
        toggleMoveEverything();

        String blobName = UtilsTest.createRandomBlobName();
        blobStore.putBlob(containerName, UtilsTest.makeBlob(blobStore, blobName));
        Blob blob = blobStore.getBlob(containerName, blobName);
        BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getTotalObjectCount()).isEqualTo(1);
        assertThat(status.getMovedObjectCount()).isEqualTo(1);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);
        UtilsTest.assertEqualBlobs(blob, blobStore.getBlob(containerName, blobName));
    }

    @Test
    public void testMoveEverythingTwice() throws Exception {
        toggleMoveEverything();

        String blobName = UtilsTest.createRandomBlobName();
        blobStore.putBlob(containerName, UtilsTest.makeBlob(blobStore, blobName));
        Blob blob = blobStore.getBlob(containerName, blobName);
        BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getTotalObjectCount()).isEqualTo(1);
        assertThat(status.getMovedObjectCount()).isEqualTo(1);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);

        status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getTotalObjectCount()).isEqualTo(1);
        assertThat(status.getMovedObjectCount()).isEqualTo(0);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);
        UtilsTest.assertEqualBlobs(blob, blobStore.getBlob(containerName, blobName));
    }

    @Test
    public void testLastModifiedTimePolicy() throws Exception {
        BouncePolicy p = lastModifiedTimePolicy(Duration.ofHours(1));
        Blob blob = UtilsTest.makeBlob(blobStore, UtilsTest.createRandomBlobName());
        blob.getMetadata().setLastModified(Date.from(bounceService.getClock().instant()));
        assertThat(((LastModifiedTimePolicy) p).isObjectExpired(blob.getMetadata())).isFalse();

        UtilsTest.advanceServiceClock(bounceService, Duration.ofHours(2));

        assertThat(((LastModifiedTimePolicy) p).isObjectExpired(blob.getMetadata())).isTrue();
    }

    @Test
    public void testBounceLastModifiedTimePolicy() throws Exception {
        UtilsTest.switchPolicyforContainer(app, containerName, LastModifiedTimePolicy.class,
                ImmutableMap.of(LastModifiedTimePolicy.DURATION, Duration.ofHours(1).toString()));

        Blob blob = UtilsTest.makeBlob(blobStore, UtilsTest.createRandomBlobName());
        blobStore.putBlob(containerName, blob);

        BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getTotalObjectCount()).isEqualTo(1);
        assertThat(status.getMovedObjectCount()).isEqualTo(0);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);

        UtilsTest.advanceServiceClock(bounceService, Duration.ofHours(2));
        status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getTotalObjectCount()).isEqualTo(1);
        assertThat(status.getMovedObjectCount()).isEqualTo(1);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);
    }

    @Test
    public void testCopyBouncePolicy() throws Exception {
        UtilsTest.switchPolicyforContainer(app, containerName, CopyPolicy.class);
        Blob blob = UtilsTest.makeBlob(blobStore, UtilsTest.createRandomBlobName());
        blobStore.putBlob(containerName, blob);
        BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getCopiedObjectCount()).isEqualTo(1);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);
        checkCopiedBlob(blob.getMetadata().getName());
    }

    @Test
    public void testCopyTwice() throws Exception {
        UtilsTest.switchPolicyforContainer(app, containerName, CopyPolicy.class);
        Blob blob = UtilsTest.makeBlob(blobStore, UtilsTest.createRandomBlobName());
        blobStore.putBlob(containerName, blob);
        BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getCopiedObjectCount()).isEqualTo(1);
        status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getCopiedObjectCount()).isEqualTo(0);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);
        checkCopiedBlob(blob.getMetadata().getName());
    }

    @Test
    public void testCopyAfterMove() throws Exception {
        UtilsTest.switchPolicyforContainer(app, containerName, MoveEverythingPolicy.class);
        Blob blob = UtilsTest.makeBlob(blobStore, UtilsTest.createRandomBlobName());
        blobStore.putBlob(containerName, blob);
        BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getMovedObjectCount()).isEqualTo(1);
        UtilsTest.switchPolicyforContainer(app, containerName, CopyPolicy.class);
        status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getCopiedObjectCount()).isEqualTo(0);
    }

    @Test
    public void testMoveAfterCopy() throws Exception {
        UtilsTest.switchPolicyforContainer(app, containerName, CopyPolicy.class);
        Blob blob = UtilsTest.makeBlob(blobStore, UtilsTest.createRandomBlobName());
        blobStore.putBlob(containerName, blob);
        BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getCopiedObjectCount()).isEqualTo(1);

        UtilsTest.switchPolicyforContainer(app, containerName, MoveEverythingPolicy.class);
        status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getMovedObjectCount()).isEqualTo(0);
        assertThat(status.getTotalObjectCount()).isEqualTo(1);
        Blob nearBlob = blobStore.getFromNearStore(containerName, blob.getMetadata().getName());
        assertThat(BounceLink.isLink(nearBlob.getMetadata())).isTrue();
    }

    private void checkCopiedBlob(String blobName) throws Exception {
        Blob nearBlob = blobStore.getFromNearStore(containerName, blobName);
        Blob farBlob = blobStore.getFromFarStore(containerName, blobName);
        try (InputStream nearPayload = nearBlob.getPayload().openStream();
             InputStream farPayload = farBlob.getPayload().openStream()) {
            assertThat(nearPayload).hasContentEqualTo(farPayload);
        }
    }

    private void toggleBounceNothing() {
        UtilsTest.switchPolicyforContainer(app, containerName, BounceNothingPolicy.class);
    }

    private void toggleMoveEverything() {
        UtilsTest.switchPolicyforContainer(app, containerName, MoveEverythingPolicy.class);
    }

    private BouncePolicy lastModifiedTimePolicy(Duration duration) {
        BouncePolicy p = new LastModifiedTimePolicy();
        Properties properties = new Properties();
        properties.putAll(ImmutableMap.of(
                LastModifiedTimePolicy.DURATION, duration.toString()
        ));
        p.init(app, new MapConfiguration(properties));
        return p;
    }
}
