/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.BounceApplication;
import com.bouncestorage.bounce.admin.BounceService;

import org.apache.commons.configuration.MapConfiguration;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class MigrationPolicyTest {
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

        synchronized (BounceApplication.class) {
            app = new BounceApplication(new MapConfiguration(new HashMap<>()));
        }
        app.useRandomPorts();
        bounceService = app.getBounceService();
        UtilsTest.switchPolicyforContainer(app, containerName, MigrationPolicy.class);
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
    public void testListDestinationOnly() throws Exception {
        String[] blobNames = {"abc", "def", "ghi"};
        putBlobs(blobNames);
        verifyList(blobNames);
    }

    @Test
    public void testListSourceOnly() throws Exception {
        String[] blobNames = {"abc", "def", "ghi"};
        putSourceBlobs(blobNames);
        verifyList(blobNames);
    }

    @Test
    public void testListOverlappingSourceDestination() throws Exception {
        String[] sourceBlobNames = {"10", "30", "50"};
        String[] destinationBlobNames = {"20", "40", "60"};
        String[] combinedNames = {"10", "20", "30", "40", "50", "60"};

        putSourceBlobs(sourceBlobNames);
        putBlobs(destinationBlobNames);
        verifyList(combinedNames);
    }

    @Test
    public void testListLongSourceNames() throws Exception {
        String[] sourceBlobNames = {"20", "40", "50", "60"};
        String[] destinationBlobNames = {"30"};
        String[] combinedNames = {"20", "30", "40", "50", "60"};

        putSourceBlobs(sourceBlobNames);
        putBlobs(destinationBlobNames);
        verifyList(combinedNames);
    }

    @Test
    public void testListLongDestinationNames() throws Exception {
        String[] destinationBlobNames = {"20", "40", "50", "60"};
        String[] sourceBlobNames = {"30"};
        String[] combinedNames = {"20", "30", "40", "50", "60"};

        putSourceBlobs(sourceBlobNames);
        putBlobs(destinationBlobNames);
        verifyList(combinedNames);
    }

    @Test
    public void testMigration() throws Exception {
        String[] blobs = {"a", "b", "c"};
        putSourceBlobs(blobs);

        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();

        assertThat(status.getMovedObjectCount()).isEqualTo(blobs.length);
        verifyList(blobs);
    }

    @Test
    public void testRemoveCopiedBlob() throws Exception {
        String[] blobs = {"a", "b", "c"};
        putSourceBlobs(blobs);
        UtilsTest.switchPolicyforContainer(app, containerName, CopyPolicy.class);
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getCopiedObjectCount()).isEqualTo(blobs.length);
        UtilsTest.switchPolicyforContainer(app, containerName, MigrationPolicy.class);

        status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getRemovedObjectCount()).isEqualTo(blobs.length);
        verifyList(blobs);
    }

    @Test
    public void testNOOPForMigratedBlobs() throws Exception {
        String[] blobs = {"a", "b", "c"};
        putBlobs(blobs);
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getCopiedObjectCount()).isEqualTo(0);
        assertThat(status.getRemovedObjectCount()).isEqualTo(0);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);
        assertThat(status.getMovedObjectCount()).isEqualTo(0);
        assertThat(status.getLinkedObjectCount()).isEqualTo(0);
    }

    @Test
    public void testListLimits() throws Exception {
        String[] blobs = {"a", "b", "c"};
        putBlobs(blobs);
        ArrayList<String> results = new ArrayList<>();
        PageSet<? extends StorageMetadata> listResults = blobStore.list(containerName, ListContainerOptions.Builder
                .maxResults(2));
        listResults.stream().forEach(meta -> results.add(meta.getName()));
        String marker = listResults.getNextMarker();
        assertThat(marker).isEqualTo("b");
        listResults = blobStore.list(containerName, ListContainerOptions.Builder.afterMarker(marker));
        listResults.stream().forEach(meta -> results.add(meta.getName()));
        assertThat(results.size()).isEqualTo(blobs.length);
        for (int i = 0; i < blobs.length; i++) {
            assertThat(results.get(i)).isEqualTo(blobs[i]);
        }
    }
    @Test
    public void testListMarker() throws Exception {
        String[] blobs = {"a", "b", "c"};
        putBlobs(blobs);
        ArrayList<String> results = new ArrayList<>();
        PageSet<? extends StorageMetadata> listResults = blobStore.list(containerName, ListContainerOptions.Builder
                .maxResults(3));
        listResults.stream().forEach(meta -> results.add(meta.getName()));
        String marker = listResults.getNextMarker();
        assertThat(marker).isEqualTo(null);
        assertThat(results.size()).isEqualTo(blobs.length);
        for (int i = 0; i < blobs.length; i++) {
            assertThat(results.get(i)).isEqualTo(blobs[i]);
        }
    }

    @Test
    public void testListSourceMarker() throws Exception {
        String[] blobs = {"a", "b", "c"};
        putSourceBlobs(blobs);
        ArrayList<String> results = new ArrayList<>();
        PageSet<? extends StorageMetadata> listResults = blobStore.list(containerName, ListContainerOptions.Builder
                .maxResults(3));
        listResults.stream().forEach(meta -> results.add(meta.getName()));
        String marker = listResults.getNextMarker();
        assertThat(marker).isEqualTo(null);
        assertThat(results.size()).isEqualTo(blobs.length);
        for (int i = 0; i < blobs.length; i++) {
            assertThat(results.get(i)).isEqualTo(blobs[i]);
        }
    }

    private void verifyList(String[] expectedNames) throws Exception {
        ArrayList<String> listedBlobNames = new ArrayList<>();
        blobStore.list(containerName).stream().forEach(meta -> listedBlobNames.add(meta.getName()));
        assertThat(listedBlobNames.size()).isEqualTo(expectedNames.length);
        for (int i = 0; i < expectedNames.length; i++) {
            assertThat(expectedNames[i]).isEqualTo(listedBlobNames.get(i));
        }
    }

    private void putBlobs(String[] destinationNames) throws Exception {
        for (String name : destinationNames) {
            Blob blob = UtilsTest.makeBlob(blobStore, name);
            blobStore.putBlob(containerName, blob);
        }
    }

    private void putSourceBlobs(String[] sourceNames) throws Exception {
        UtilsTest.switchPolicyforContainer(app, containerName, BounceNothingPolicy.class);
        putBlobs(sourceNames);
        UtilsTest.switchPolicyforContainer(app, containerName, MigrationPolicy.class);
    }
}
