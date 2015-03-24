/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.BounceApplication;
import com.bouncestorage.bounce.admin.BounceService;
import com.google.common.io.ByteSource;

import org.apache.commons.configuration.MapConfiguration;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class CopyPolicyTest {
    String containerName;
    BlobStoreContext bounceContext;
    BounceBlobStore blobStore;
    BounceService bounceService;

    @Before
    public void setUp() throws Exception {
        containerName = UtilsTest.createRandomContainerName();

        bounceContext = UtilsTest.createTransientBounceBlobStore();
        blobStore = (BounceBlobStore) bounceContext.getBlobStore();
        blobStore.createContainerInLocation(null, containerName);

        BounceApplication app;
        synchronized (BounceApplication.class) {
            app = new BounceApplication(new MapConfiguration(new HashMap<>()));
        }
        app.useRandomPorts();
        app.useBlobStore(blobStore);
        bounceService = app.getBounceService();
        bounceService.setDefaultPolicy(new CopyPolicy());
    }

    @After
    public void tearDown() {
        if (blobStore != null) {
            blobStore.deleteContainer(containerName);
        }

        if (bounceContext != null) {
            bounceContext.close();
        }
    }

    @Test
    public void testCopyObject() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        blobStore.putBlob(containerName, UtilsTest.makeBlob(blobStore, blobName));
        assertThat(blobStore.getFromFarStore(containerName, blobName)).isNull();
        assertThat(blobStore.getFromNearStore(containerName, blobName)).isNotNull();
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(blobStore.getFromNearStore(containerName, blobName)).isNotNull();
        assertThat(blobStore.getFromFarStore(containerName, blobName)).isNotNull();
        BlobMetadata source = blobStore.blobMetadataNoFollow(containerName, blobName);
        assertThat(BounceLink.isLink(source)).isFalse();
        Blob far = blobStore.getFromFarStore(containerName, blobName);
        Blob near = blobStore.getFromNearStore(containerName, blobName);
        UtilsTest.assertEqualBlobs(near, far);
        assertThat(status.getCopiedObjectCount()).isEqualTo(1);
    }

    @Test
    public void testCopyLink() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        Blob blob = UtilsTest.makeBlob(blobStore, blobName);
        blobStore.putBlob(containerName, blob);
        assertThat(blobStore.getFromFarStore(containerName, blobName)).isNull();
        assertThat(blobStore.getFromNearStore(containerName, blobName)).isNotNull();
        bounceService.setDefaultPolicy(new MoveEverythingPolicy());
        bounceService.bounce(containerName).future().get();
        assertThat(blobStore.getFromNearStore(containerName, blobName)).isNotNull();
        assertThat(blobStore.getFromFarStore(containerName, blobName)).isNotNull();
        BlobMetadata source = blobStore.blobMetadataNoFollow(containerName, blobName);
        assertThat(BounceLink.isLink(source)).isTrue();
        bounceService.setDefaultPolicy(new CopyPolicy());
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        Blob far = blobStore.getFromFarStore(containerName, blobName);
        UtilsTest.assertEqualBlobs(blob, far);
        assertThat(status.getCopiedObjectCount()).isEqualTo(0);
    }

    @Test
    public void testReconcilingDifferentBlobs() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        Blob blobFoo = UtilsTest.makeBlob(blobStore, blobName, ByteSource.wrap("foo".getBytes()));
        Blob blobBar = UtilsTest.makeBlob(blobStore, blobName, ByteSource.wrap("bar".getBytes()));

        // Place a blob with contents "foo" into the blob store and copy it
        blobStore.putBlob(containerName, blobFoo);
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getCopiedObjectCount()).isEqualTo(1);

        // Place a blob with contents "bar" with the same name into the blob store
        Blob far = blobStore.getFromFarStore(containerName, blobName);
        UtilsTest.assertEqualBlobs(blobFoo, far);
        blobStore.putBlob(containerName, blobBar);
        Blob near = blobStore.getFromNearStore(containerName, blobName);
        UtilsTest.assertEqualBlobs(blobBar, near);

        // Copy blob -- "bar" should be copied to the far store
        status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getCopiedObjectCount()).isEqualTo(1);
        far = blobStore.getFromFarStore(containerName, blobName);
        near = blobStore.getFromNearStore(containerName, blobName);
        UtilsTest.assertEqualBlobs(blobBar, far);
        UtilsTest.assertEqualBlobs(blobBar, near);
    }

    @Test
    public void testReconcileRemoveBlob() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        Blob blobFoo = UtilsTest.makeBlob(blobStore, blobName, ByteSource.wrap("foo".getBytes()));

        // Place the blob "foo" into the blob store and copy it
        blobStore.putBlob(containerName, blobFoo);
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getCopiedObjectCount()).isEqualTo(1);
        Blob farBlob = blobStore.getFromFarStore(containerName, blobName);

        // Delete the blob "foo" and check that it was removed from the near store
        blobStore.removeBlob(containerName, blobName);
        assertThat(blobStore.getFromNearStore(containerName, blobName)).isNull();

        status = bounceService.bounce(containerName);
        status.future().get();

        assertThat(status.getRemovedObjectCount()).isEqualTo(1);
        assertThat(blobStore.getFromFarStore(containerName, blobName)).isNull();
    }
}
