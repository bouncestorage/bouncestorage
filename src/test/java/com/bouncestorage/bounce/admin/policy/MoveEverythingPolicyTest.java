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

public final class MoveEverythingPolicyTest {
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
        bounceService.setDefaultPolicy(new MoveEverythingPolicy());
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
    public void testMoveObject() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        Blob blob = UtilsTest.makeBlob(blobStore, blobName);
        blobStore.putBlob(containerName, blob);
        assertThat(blobStore.getFromFarStore(containerName, blobName)).isNull();
        assertThat(blobStore.getFromNearStore(containerName, blobName)).isNotNull();
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(blobStore.getFromNearStore(containerName, blobName)).isNotNull();
        assertThat(blobStore.getFromFarStore(containerName, blobName)).isNotNull();
        BlobMetadata source = blobStore.blobMetadataNoFollow(containerName, blobName);
        assertThat(BounceLink.isLink(source)).isTrue();
        assertThat(status.getMovedObjectCount()).isEqualTo(1);
        Blob linkedBlob = blobStore.getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(linkedBlob, blob);
    }

    @Test
    public void testCopiedBlobChangedToLink() throws Exception {
        // Check that after a blob is copied, MoveEverything policy will create a link
        String blobName = UtilsTest.createRandomBlobName();
        Blob blob = UtilsTest.makeBlob(blobStore, blobName);
        blobStore.putBlob(containerName, blob);

        // Copy the blob over
        blobStore.setPolicy(new CopyPolicy());
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        Blob farBlob = blobStore.getFromFarStore(containerName, blobName);
        Blob nearBlob = blobStore.getFromNearStore(containerName, blobName);
        UtilsTest.assertEqualBlobs(nearBlob, farBlob);

        // Run the move policy and ensure that a link is created
        bounceService.setDefaultPolicy(new MoveEverythingPolicy());
        status = bounceService.bounce(containerName);
        status.future().get();
        BlobMetadata source = blobStore.blobMetadataNoFollow(containerName, blobName);
        assertThat(BounceLink.isLink(source)).isTrue();
        assertThat(status.getLinkedObjectCount()).isEqualTo(1);
        Blob linkedBlob = blobStore.getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(linkedBlob, blob);
    }

    @Test
    public void testRemoveFarBlobWithoutLink() throws Exception {
        // Check that after a link is removed, the blob in the far store is removed as well
        String blobName = UtilsTest.createRandomBlobName();
        Blob blob = UtilsTest.makeBlob(blobStore, blobName);
        blobStore.putBlob(containerName, blob);

        // Move the blob over
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        Blob farBlob = blobStore.getFromFarStore(containerName, blobName);
        UtilsTest.assertEqualBlobs(blob, farBlob);
        assertThat(status.getMovedObjectCount()).isEqualTo(1);

        // Run the move policy and ensure that the blob is removed
        blobStore.removeBlob(containerName, blobName);
        farBlob = blobStore.getFromFarStore(containerName, blobName);
        assertThat(blobStore.getBlob(containerName, blobName)).isNull();
        UtilsTest.assertEqualBlobs(farBlob, blob);
        status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getRemovedObjectCount()).isEqualTo(1);
        assertThat(blobStore.getFromFarStore(containerName, blobName)).isNull();
    }

    @Test
    public void testOverwriteLink() throws Exception {
        // Check that after moving a blob, a PUT to the same key will force a move
        String blobName = UtilsTest.createRandomBlobName();
        Blob blobFoo = UtilsTest.makeBlob(blobStore, blobName, ByteSource.wrap("foo".getBytes()));
        Blob blobBar = UtilsTest.makeBlob(blobStore, blobName, ByteSource.wrap("bar".getBytes()));
        blobStore.putBlob(containerName, blobFoo);

        // Move the blob
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        Blob farBlob = blobStore.getFromFarStore(containerName, blobName);
        UtilsTest.assertEqualBlobs(blobFoo, farBlob);
        assertThat(status.getMovedObjectCount()).isEqualTo(1);

        // Update the object
        blobStore.putBlob(containerName, blobBar);
        Blob nearBlob = blobStore.getFromNearStore(containerName, blobName);
        Blob canonicalBlob = blobStore.getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(nearBlob, blobBar);
        UtilsTest.assertEqualBlobs(farBlob, blobFoo);
        UtilsTest.assertEqualBlobs(canonicalBlob, blobBar);

        // Run the move policy and ensure that the blob is updated
        status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getMovedObjectCount()).isEqualTo(1);
        farBlob = blobStore.getFromFarStore(containerName, blobName);
        UtilsTest.assertEqualBlobs(farBlob, blobBar);
    }
}
