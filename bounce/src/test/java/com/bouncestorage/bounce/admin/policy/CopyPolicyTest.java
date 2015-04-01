/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static org.assertj.core.api.Assertions.assertThat;

import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.BounceApplication;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.bouncestorage.bounce.admin.BounceService;
import com.google.common.io.ByteSource;

import org.apache.commons.configuration.Configuration;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class CopyPolicyTest {
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
        bounceService = new BounceService(app);

        Configuration config = app.getConfiguration();
        UtilsTest.createTransientProviderConfig(config);
        UtilsTest.createTransientProviderConfig(config);
        UtilsTest.switchPolicyforContainer(app, containerName, CopyPolicy.class);
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
    public void testCopyObject() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        policy.putBlob(containerName, UtilsTest.makeBlob(policy, blobName));
        assertThat(policy.getDestination().blobExists(containerName, blobName)).isFalse();
        assertThat(policy.getSource().blobExists(containerName, blobName)).isTrue();
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(policy.getDestination().blobExists(containerName, blobName)).isTrue();
        assertThat(policy.getSource().blobExists(containerName, blobName)).isTrue();
        BlobMetadata source = policy.getSource().blobMetadata(containerName, blobName);
        assertThat(BounceLink.isLink(source)).isFalse();
        Blob far = policy.getDestination().getBlob(containerName, blobName);
        Blob near = policy.getSource().getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(near, far);
        assertThat(status.getCopiedObjectCount()).isEqualTo(1);
    }

    @Test
    public void testCopyLink() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        Blob blob = UtilsTest.makeBlob(policy, blobName);
        policy.putBlob(containerName, blob);
        assertThat(policy.getDestination().blobExists(containerName, blobName)).isFalse();
        assertThat(policy.getSource().blobExists(containerName, blobName)).isTrue();

        Utils.copyBlobAndCreateBounceLink(policy.getSource(), policy.getDestination(), containerName, blobName);
        assertThat(policy.getSource().blobExists(containerName, blobName)).isTrue();
        assertThat(policy.getDestination().blobExists(containerName, blobName)).isTrue();
        BlobMetadata source = policy.getSource().blobMetadata(containerName, blobName);
        assertThat(source).isNotNull();
        assertThat(BounceLink.isLink(source)).isTrue();

        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        Blob far = policy.getDestination().getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(blob, far);
        assertThat(status.getCopiedObjectCount()).isEqualTo(0);
    }

    @Test
    public void testReconcilingDifferentBlobs() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        Blob blobFoo = UtilsTest.makeBlob(policy, blobName, ByteSource.wrap("foo".getBytes()));
        Blob blobBar = UtilsTest.makeBlob(policy, blobName, ByteSource.wrap("bar".getBytes()));

        // Place a blob with contents "foo" into the blob store and copy it
        policy.putBlob(containerName, blobFoo);
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getCopiedObjectCount()).isEqualTo(1);

        // Place a blob with contents "bar" with the same name into the blob store
        Blob far = policy.getDestination().getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(blobFoo, far);
        policy.putBlob(containerName, blobBar);
        Blob near = policy.getSource().getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(blobBar, near);

        // Copy blob -- "bar" should be copied to the far store
        status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getCopiedObjectCount()).isEqualTo(1);
        far = policy.getDestination().getBlob(containerName, blobName);
        near = policy.getSource().getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(blobBar, far);
        UtilsTest.assertEqualBlobs(blobBar, near);
    }

    @Test
    public void testReconcileRemoveBlob() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        Blob blobFoo = UtilsTest.makeBlob(policy, blobName, ByteSource.wrap("foo".getBytes()));

        // Place the blob "foo" into the blob store and copy it
        policy.putBlob(containerName, blobFoo);
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getCopiedObjectCount()).isEqualTo(1);
        Blob farBlob = policy.getDestination().getBlob(containerName, blobName);

        // Delete the blob "foo" and check that it was removed from the near store
        policy.removeBlob(containerName, blobName);
        assertThat(policy.getSource().blobExists(containerName, blobName)).isFalse();

        status = bounceService.bounce(containerName);
        status.future().get();

        assertThat(status.getRemovedObjectCount()).isEqualTo(1);
        assertThat(policy.getDestination().blobExists(containerName, blobName)).isFalse();
    }
}
