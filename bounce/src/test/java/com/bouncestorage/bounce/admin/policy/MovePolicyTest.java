/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static org.assertj.core.api.Assertions.assertThat;

import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.BounceApplication;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.bouncestorage.bounce.admin.BounceService;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class MovePolicyTest {
    String containerName;
    BouncePolicy policy;
    BounceService bounceService;
    BounceApplication app;

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
        UtilsTest.switchPolicyforContainer(app, containerName, MoveEverythingPolicy.class);
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
    public void testBringBackObject() throws Exception {
        // Checks that after a GET, the object is put back into the "source" store
        String blobName = UtilsTest.createRandomBlobName();
        Blob blob = UtilsTest.makeBlob(policy, blobName);
        policy.putBlob(containerName, blob);
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(policy.getDestination().blobExists(containerName, blobName)).isTrue();
        assertThat(policy.getSource().blobExists(containerName, blobName)).isTrue();
        BlobMetadata source = policy.getSource().blobMetadata(containerName, blobName);
        assertThat(BounceLink.isLink(source)).isTrue();
        assertThat(status.getMovedObjectCount()).isEqualTo(1);
        Blob linkedBlob = policy.getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(linkedBlob, blob);

        Blob retrievedBlob = policy.getBlob(containerName, blobName);
        Blob nearBlob = policy.getSource().getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(retrievedBlob, blob);
        UtilsTest.assertEqualBlobs(nearBlob, blob);
        assertThat(BounceLink.isLink(policy.getSource().blobMetadata(containerName, blobName))).isFalse();
    }
}
