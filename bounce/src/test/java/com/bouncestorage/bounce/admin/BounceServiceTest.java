/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static com.bouncestorage.bounce.UtilsTest.assertStatus;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Calendar;

import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.BounceService.BounceTaskStatus;
import com.bouncestorage.bounce.admin.policy.NoBouncePolicy;
import com.bouncestorage.bounce.admin.policy.WriteBackPolicy;
import com.google.common.collect.ImmutableMap;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BounceServiceTest {
    private Logger logger;
    private BouncePolicy policy;
    private String containerName;
    private BounceService bounceService;
    private BounceApplication app;

    @Before
    public void setUp() throws Exception {
        containerName = Utils.createRandomContainerName();

        synchronized (BounceApplication.class) {
            app = new BounceApplication();
            logger = LoggerFactory.getLogger(getClass());
        }
        app.useRandomPorts();
        app.registerConfigurationListener();
        app.pauseBackgroundTasks();
        bounceService = app.bounceService = new BounceService(app);

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
        assertStatus(status, status::getTotalObjectCount).isEqualTo(1);
        assertStatus(status, status::getMovedObjectCount).isEqualTo(0);
        assertStatus(status, status::getErrorObjectCount).isEqualTo(0);
    }

    @Test
    public void testMoveEverythingPolicy() throws Exception {
        toggleMoveEverything();

        String blobName = UtilsTest.createRandomBlobName();
        policy.putBlob(containerName, UtilsTest.makeBlob(policy, blobName));
        Blob blob = policy.getBlob(containerName, blobName);
        BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertStatus(status, status::getTotalObjectCount).isEqualTo(1);
        assertStatus(status, status::getMovedObjectCount).isEqualTo(1);
        assertStatus(status, status::getErrorObjectCount).isEqualTo(0);
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
        assertStatus(status, status::getTotalObjectCount).isEqualTo(1);
        assertStatus(status, status::getMovedObjectCount).isEqualTo(1);
        assertStatus(status, status::getErrorObjectCount).isEqualTo(0);

        status = bounceService.bounce(containerName);
        status.future().get();
        assertStatus(status, status::getTotalObjectCount).isEqualTo(1);
        assertStatus(status, status::getMovedObjectCount).isEqualTo(0);
        assertStatus(status, status::getErrorObjectCount).isEqualTo(0);
        UtilsTest.assertEqualBlobs(blob, policy.getBlob(containerName, blobName));
    }

    @Test
    public void testBounceLastModifiedTimePolicy() throws Exception {
        lastModifiedTimePolicy(Duration.ofHours(1));

        Blob blob = UtilsTest.makeBlob(policy, UtilsTest.createRandomBlobName());
        policy.putBlob(containerName, blob);

        BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertStatus(status, status::getTotalObjectCount).isEqualTo(1);
        assertStatus(status, status::getMovedObjectCount).isEqualTo(0);
        assertStatus(status, status::getErrorObjectCount).isEqualTo(0);

        UtilsTest.advanceServiceClock(app, Duration.ofHours(2));
        status = bounceService.bounce(containerName);
        status.future().get();
        assertStatus(status, status::getTotalObjectCount).isEqualTo(1);
        assertStatus(status, status::getMovedObjectCount).isEqualTo(1);
        assertStatus(status, status::getErrorObjectCount).isEqualTo(0);
    }

    @Test
    public void testCopyBouncePolicy() throws Exception {
        containerName = UtilsTest.switchPolicyforContainer(app, WriteBackPolicy.class,
                ImmutableMap.of(WriteBackPolicy.COPY_DELAY, Duration.ofSeconds(0).toString(),
                        WriteBackPolicy.EVICT_DELAY, Duration.ofSeconds(-1).toString()));
        policy = (BouncePolicy) app.getBlobStore(containerName);

        Blob blob = UtilsTest.makeBlob(policy, UtilsTest.createRandomBlobName());
        policy.putBlob(containerName, blob);
        BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertStatus(status, status::getCopiedObjectCount).isEqualTo(1);
        assertStatus(status, status::getErrorObjectCount).isEqualTo(0);
        checkCopiedBlob(blob.getMetadata().getName());
    }

    @Test
    public void testCopyTwice() throws Exception {
        containerName = UtilsTest.switchPolicyforContainer(app, WriteBackPolicy.class,
                ImmutableMap.of(WriteBackPolicy.COPY_DELAY, Duration.ofSeconds(0).toString(),
                        WriteBackPolicy.EVICT_DELAY, Duration.ofSeconds(-1).toString()));

        policy = (BouncePolicy) app.getBlobStore(containerName);

        Blob blob = UtilsTest.makeBlob(policy, UtilsTest.createRandomBlobName());
        policy.putBlob(containerName, blob);
        BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertStatus(status, status::getCopiedObjectCount).isEqualTo(1);
        status = bounceService.bounce(containerName);
        status.future().get();
        assertStatus(status, status::getCopiedObjectCount).isEqualTo(0);
        assertStatus(status, status::getErrorObjectCount).isEqualTo(0);
        checkCopiedBlob(blob.getMetadata().getName());
    }

    @Test
    public void testCopyAfterMove() throws Exception {
        containerName = UtilsTest.switchPolicyforContainer(app, WriteBackPolicy.class,
                ImmutableMap.of(WriteBackPolicy.COPY_DELAY, Duration.ofSeconds(0).toString(),
                        WriteBackPolicy.EVICT_DELAY, Duration.ofSeconds(-1).toString()));

        policy = (BouncePolicy) app.getBlobStore(containerName);

        Blob blob = UtilsTest.makeBlob(policy, UtilsTest.createRandomBlobName());
        policy.getSource().putBlob(containerName, blob);
        Utils.copyBlobAndCreateBounceLink(policy.getSource(), policy.getDestination(), containerName,
                blob.getMetadata().getName());

        BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertStatus(status, status::getCopiedObjectCount).isEqualTo(0);
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
        assertStatus(status, status::getMovedObjectCount).isEqualTo(0);
        assertStatus(status, status::getTotalObjectCount).isEqualTo(1);
        BlobMetadata nearBlob = policy.getSource().blobMetadata(containerName, blob.getMetadata().getName());
        assertThat(BounceLink.isLink(nearBlob)).isTrue();
    }


    @Test
    public void testScheduledBounce() throws Exception {
        toggleMoveEverything();

        String blobName = UtilsTest.createRandomBlobName();
        policy.putBlob(containerName, UtilsTest.makeBlob(policy, blobName));
        Blob blob = policy.getBlob(containerName, blobName);

        Calendar calendar = new Calendar.Builder()
                .setInstant(app.getClock().instant().getEpochSecond() * 1000)
                .build();
        calendar.set(Calendar.HOUR_OF_DAY, BounceApplication.BOUNCE_SCHEDULE_TIME);
        calendar.set(Calendar.DAY_OF_YEAR, calendar.get(Calendar.DAY_OF_YEAR) + 1);
        app.setClock(Clock.fixed(calendar.toInstant(), ZoneId.systemDefault()));

        app.startBounceScheduler();
        BounceTaskStatus status;

        Utils.waitUntil(() -> bounceService.status(containerName) != null);
        status = bounceService.status(containerName);
        status.future().get();
        assertStatus(status, status::getTotalObjectCount).isEqualTo(1);
        assertStatus(status, status::getMovedObjectCount).isEqualTo(1);
        assertStatus(status, status::getErrorObjectCount).isEqualTo(0);
        UtilsTest.assertEqualBlobs(blob, policy.getBlob(containerName, blobName));
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
        containerName = UtilsTest.switchPolicyforContainer(app, NoBouncePolicy.class);
        policy = (BouncePolicy) app.getBlobStore(containerName);
    }

    private void toggleMoveEverything() {
        lastModifiedTimePolicy(Duration.ofSeconds(0));

        policy = (BouncePolicy) app.getBlobStore(containerName);
        policy.createContainerInLocation(null, containerName);
    }

    private void lastModifiedTimePolicy(Duration duration) {
        containerName = UtilsTest.switchPolicyforContainer(app, WriteBackPolicy.class,
                ImmutableMap.of(WriteBackPolicy.COPY_DELAY, Duration.ofSeconds(-1).toString(),
                        WriteBackPolicy.EVICT_DELAY, duration.toString()));

        policy = (BouncePolicy) app.getBlobStore(containerName);
    }
}
