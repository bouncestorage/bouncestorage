/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import java.time.Duration;

import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.BounceApplication;
import com.bouncestorage.bounce.admin.BouncePolicy;

import org.jclouds.blobstore.domain.Blob;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class WriteBackPolicyAutoWriteBackTest {
    String containerName;
    BounceApplication app;
    BouncePolicy policy;

    @Before
    public void setUp() throws Exception {
        containerName = UtilsTest.createRandomContainerName();

        synchronized (BounceApplication.class) {
            app = new BounceApplication();
        }
        app.useRandomPorts();
        app.registerConfigurationListener();

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
}
