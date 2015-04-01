/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.time.Duration;
import java.util.Date;

import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.BounceService.BounceTaskStatus;
import com.bouncestorage.bounce.admin.policy.BounceNothingPolicy;
import com.bouncestorage.bounce.admin.policy.CopyPolicy;
import com.bouncestorage.bounce.admin.policy.LastModifiedTimePolicy;
import com.bouncestorage.bounce.admin.policy.MoveEverythingPolicy;
import com.google.common.collect.ImmutableMap;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class BounceServiceTest {
    private BouncePolicy policy;
    private String containerName;
    private BounceService bounceService;
    private BounceApplication app;

    @Before
    public void setUp() throws Exception {
        containerName = UtilsTest.createRandomContainerName();

        synchronized (BounceApplication.class) {
            app = new BounceApplication();
        }
        app.useRandomPorts();
        bounceService = new BounceService(app);

        UtilsTest.createTestProvidersConfig(app.getConfiguration());
    }

    @After
    public void tearDown() throws Exception {
        if (policy != null) {
            policy.deleteContainer(containerName);
        }
    }

    @Test
    public void testBounceNothingPolicy() throws Exception {
        toggleBounceNothing();

        policy.putBlob(containerName,
                UtilsTest.makeBlob(policy, UtilsTest.createRandomBlobName()));
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
        policy.putBlob(containerName, UtilsTest.makeBlob(policy, blobName));
        Blob blob = policy.getBlob(containerName, blobName);
        BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getTotalObjectCount()).isEqualTo(1);
        assertThat(status.getMovedObjectCount()).isEqualTo(1);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);
        UtilsTest.assertEqualBlobs(blob, policy.getBlob(containerName, blobName));
    }

    @Test
    public void testMoveEverythingTwice() throws Exception {
        toggleMoveEverything();

        String blobName = UtilsTest.createRandomBlobName();
        policy.putBlob(containerName, UtilsTest.makeBlob(policy, blobName));
        Blob blob = policy.getBlob(containerName, blobName);
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
        UtilsTest.assertEqualBlobs(blob, policy.getBlob(containerName, blobName));
    }

    @Test
    public void testLastModifiedTimePolicy() throws Exception {
        lastModifiedTimePolicy(Duration.ofHours(1));
        Blob blob = UtilsTest.makeBlob(policy, UtilsTest.createRandomBlobName());
        blob.getMetadata().setLastModified(Date.from(bounceService.getClock().instant()));
        assertThat(((LastModifiedTimePolicy) policy).isObjectExpired(blob.getMetadata())).isFalse();

        UtilsTest.advanceServiceClock(app, Duration.ofHours(2));

        assertThat(((LastModifiedTimePolicy) policy).isObjectExpired(blob.getMetadata())).isTrue();
    }

    @Test
    public void testBounceLastModifiedTimePolicy() throws Exception {
        lastModifiedTimePolicy(Duration.ofHours(1));

        Blob blob = UtilsTest.makeBlob(policy, UtilsTest.createRandomBlobName());
        policy.putBlob(containerName, blob);

        BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getTotalObjectCount()).isEqualTo(1);
        assertThat(status.getMovedObjectCount()).isEqualTo(0);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);

        UtilsTest.advanceServiceClock(app, Duration.ofHours(2));
        status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getTotalObjectCount()).isEqualTo(1);
        assertThat(status.getMovedObjectCount()).isEqualTo(1);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);
    }

    @Test
    public void testCopyBouncePolicy() throws Exception {
        UtilsTest.switchPolicyforContainer(app, containerName, CopyPolicy.class);
        policy = (BouncePolicy) app.getBlobStore(containerName);
        policy.createContainerInLocation(null, containerName);

        Blob blob = UtilsTest.makeBlob(policy, UtilsTest.createRandomBlobName());
        policy.putBlob(containerName, blob);
        BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getCopiedObjectCount()).isEqualTo(1);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);
        checkCopiedBlob(blob.getMetadata().getName());
    }

    @Test
    public void testCopyTwice() throws Exception {
        UtilsTest.switchPolicyforContainer(app, containerName, CopyPolicy.class);
        policy = (BouncePolicy) app.getBlobStore(containerName);
        policy.createContainerInLocation(null, containerName);

        Blob blob = UtilsTest.makeBlob(policy, UtilsTest.createRandomBlobName());
        policy.putBlob(containerName, blob);
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
        UtilsTest.switchPolicyforContainer(app, containerName, CopyPolicy.class);
        policy = (BouncePolicy) app.getBlobStore(containerName);
        policy.createContainerInLocation(null, containerName);

        Blob blob = UtilsTest.makeBlob(policy, UtilsTest.createRandomBlobName());
        policy.getSource().putBlob(containerName, blob);
        Utils.copyBlobAndCreateBounceLink(policy.getSource(), policy.getDestination(), containerName,
                blob.getMetadata().getName());

        BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getCopiedObjectCount()).isEqualTo(0);
    }

    @Test
    public void testMoveAfterCopy() throws Exception {
        toggleMoveEverything();

        Blob blob = UtilsTest.makeBlob(policy, UtilsTest.createRandomBlobName());
        policy.getSource().putBlob(containerName, blob);
        Utils.copyBlob(policy.getSource(), policy.getDestination(), containerName, containerName,
                blob.getMetadata().getName());

        BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getMovedObjectCount()).isEqualTo(0);
        assertThat(status.getTotalObjectCount()).isEqualTo(1);
        BlobMetadata nearBlob = policy.getSource().blobMetadata(containerName, blob.getMetadata().getName());
        assertThat(BounceLink.isLink(nearBlob)).isTrue();
    }

    private void checkCopiedBlob(String blobName) throws Exception {
        Blob nearBlob = policy.getSource().getBlob(containerName, blobName);
        Blob farBlob = policy.getDestination().getBlob(containerName, blobName);
        try (InputStream nearPayload = nearBlob.getPayload().openStream();
             InputStream farPayload = farBlob.getPayload().openStream()) {
            assertThat(nearPayload).hasContentEqualTo(farPayload);
        }
    }

    private void toggleBounceNothing() {
        UtilsTest.switchPolicyforContainer(app, containerName, BounceNothingPolicy.class);
        policy = (BouncePolicy) app.getBlobStore(containerName);
        policy.createContainerInLocation(null, containerName);
    }

    private void toggleMoveEverything() {
        UtilsTest.switchPolicyforContainer(app, containerName, MoveEverythingPolicy.class);
        policy = (BouncePolicy) app.getBlobStore(containerName);
        policy.createContainerInLocation(null, containerName);
    }

    private void lastModifiedTimePolicy(Duration duration) {
        UtilsTest.switchPolicyforContainer(app, containerName, LastModifiedTimePolicy.class,
                ImmutableMap.of(LastModifiedTimePolicy.EVICT_DELAY, duration.toString()));
        policy = (BouncePolicy) app.getBlobStore(containerName);
        policy.createContainerInLocation(null, containerName);
    }
}
