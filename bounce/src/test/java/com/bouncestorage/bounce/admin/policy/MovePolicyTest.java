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

import org.apache.commons.configuration.MapConfiguration;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class MovePolicyTest {
    String containerName;
    BlobStoreContext bounceContext;
    BounceBlobStore blobStore;
    BounceService bounceService;
    BounceApplication app;

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
        bounceService = app.getBounceService();
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
    public void testBringBackObject() throws Exception {
        // Checks that after a GET, the object is put back into the "source" store
        String blobName = UtilsTest.createRandomBlobName();
        Blob blob = UtilsTest.makeBlob(blobStore, blobName);
        blobStore.putBlob(containerName, blob);
        UtilsTest.switchPolicyforContainer(app, containerName, MoveEverythingPolicy.class);
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(blobStore.getFromNearStore(containerName, blobName)).isNotNull();
        assertThat(blobStore.getFromFarStore(containerName, blobName)).isNotNull();
        BlobMetadata source = blobStore.blobMetadataNoFollow(containerName, blobName);
        assertThat(BounceLink.isLink(source)).isTrue();
        assertThat(status.getMovedObjectCount()).isEqualTo(1);
        Blob linkedBlob = blobStore.getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(linkedBlob, blob);

        Blob retrievedBlob = blobStore.getBlob(containerName, blobName);
        Blob nearBlob = blobStore.getFromNearStore(containerName, blobName);
        UtilsTest.assertEqualBlobs(retrievedBlob, blob);
        UtilsTest.assertEqualBlobs(nearBlob, blob);
        assertThat(BounceLink.isLink(blobStore.blobMetadataNoFollow(containerName, blobName))).isFalse();
    }
}
