/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.BounceApplication;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.bouncestorage.bounce.admin.BounceService;
import com.bouncestorage.bounce.utils.Repeat;
import com.bouncestorage.bounce.utils.RepeatRule;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.options.CopyOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public final class WriteBackPolicyAutoWriteBackTest {
    @Rule
    public final RepeatRule REPEAT_RULE = new RepeatRule();
    String containerName;
    BounceApplication app;
    BouncePolicy policy;
    BounceService bounceService;

    @Before
    public void setUp() throws Exception {
        containerName = UtilsTest.createRandomContainerName();

        synchronized (BounceApplication.class) {
            app = new BounceApplication();
        }
        app.useRandomPorts();
        app.registerConfigurationListener();
        bounceService = new BounceService(app);

        UtilsTest.createTestProvidersConfig(app.getConfiguration());
        UtilsTest.useWriteBackPolicyForContainer(app, containerName,
                Duration.ofSeconds(0), Duration.ofSeconds(-1));
        policy = (BouncePolicy) app.getBlobStore(containerName);
        policy.createContainerInLocation(null, containerName);
    }

    @After
    public void tearDown() {
        if (policy != null) {
            policy.deleteContainer(containerName);
        }
    }

    @Test
    public void testAutoWriteBack() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        Blob blob = UtilsTest.makeBlob(policy, blobName);
        policy.putBlob(containerName, blob);

        app.drainBackgroundTasks();
        UtilsTest.assertEqualBlobs(policy.getSource().getBlob(containerName, blobName),
                policy.getDestination().getBlob(containerName, blobName));
    }

    @Test
    public void testAutoWriteBackCopy() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        String copyBlobName = UtilsTest.createRandomBlobName();
        Blob blob = UtilsTest.makeBlob(policy, blobName);
        policy.putBlob(containerName, blob);
        policy.copyBlob(containerName, blobName, containerName, copyBlobName, CopyOptions.NONE);

        app.drainBackgroundTasks();
        UtilsTest.assertEqualBlobs(policy.getSource().getBlob(containerName, blobName),
                policy.getDestination().getBlob(containerName, blobName));
        UtilsTest.assertEqualBlobs(policy.getSource().getBlob(containerName, copyBlobName),
                policy.getDestination().getBlob(containerName, copyBlobName));
    }

    @Test
    @Repeat(1)
    public void testAutoDelete() throws Exception {
        app.pauseBackgroundTasks();

        String blobName = UtilsTest.createRandomBlobName();
        Blob blob = UtilsTest.makeBlob(policy, blobName);
        policy.putBlob(containerName, blob);

        // ensure that we've done the copy
        app.busyWaitForBackgroundReconcileTasks();
        assertThat(policy.getDestination().blobExists(containerName, blobName)).isTrue();

        policy.removeBlob(containerName, blobName);
        app.drainBackgroundTasks();
        assertThat(policy.getDestination().blobExists(containerName, blobName)).isFalse();
    }
}
