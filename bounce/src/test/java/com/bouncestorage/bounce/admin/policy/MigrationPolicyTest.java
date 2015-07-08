/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static com.bouncestorage.bounce.UtilsTest.assertEqualBlobs;
import static com.bouncestorage.bounce.UtilsTest.assertStatus;
import static com.google.common.base.Throwables.propagate;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

import javax.ws.rs.HttpMethod;

import com.bouncestorage.bounce.BlobStoreTarget;
import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.BounceApplication;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.bouncestorage.bounce.admin.BounceService;
import com.bouncestorage.bounce.admin.BounceStats;
import com.bouncestorage.bounce.admin.StatsQueueEntry;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class MigrationPolicyTest {
    String containerName;
    BouncePolicy policy;
    BounceService bounceService;
    BounceApplication app;

    @Before
    public void setUp() throws Exception {
        synchronized (BounceApplication.class) {
            app = new BounceApplication();
        }
        app.useRandomPorts();
        app.registerConfigurationListener();
        bounceService = new BounceService(app);

        UtilsTest.createTestProvidersConfig(app.getConfiguration());
        containerName = UtilsTest.switchPolicyforContainer(app, MigrationPolicy.class);
        policy = (BouncePolicy) app.getBlobStore(containerName);
    }

    @After
    public void tearDown() {
        if (policy != null) {
            policy.deleteContainer(containerName);
        }
    }

    @Test
    public void testDeleteContainer() {
        policy.deleteContainer(containerName);
        assertThat(policy.getSource().containerExists(containerName)).isFalse();
        assertThat(policy.getDestination().containerExists(containerName)).isFalse();
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

        assertStatus(status, status::getMovedObjectCount).isEqualTo(blobs.length);
        verifyList(blobs);
    }

    @Test
    public void testOverwriteBlob() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        Blob blobFoo = UtilsTest.makeBlob(policy, blobName, ByteSource.wrap("foo".getBytes()));
        Blob blobBar = UtilsTest.makeBlob(policy, blobName, ByteSource.wrap("bar".getBytes()));

        policy.getSource().putBlob(containerName, blobFoo);
        policy.putBlob(containerName, blobBar);

        assertEqualBlobs(policy.getBlob(containerName, blobName), blobBar);
    }

    @Test
    public void testRemoveCopiedBlob() throws Exception {
        String[] blobs = {"a", "b", "c"};
        putSourceBlobs(blobs);

        Arrays.stream(blobs).forEach(b -> {
            try {
                Utils.copyBlob(policy.getSource(), policy.getDestination(), containerName, containerName, b);
            } catch (IOException e) {
                throw propagate(e);
            }
        });

        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertStatus(status, status::getRemovedObjectCount).isEqualTo(blobs.length);
        verifyList(blobs);
    }

    @Test
    public void testNOOPForMigratedBlobs() throws Exception {
        String[] blobs = {"a", "b", "c"};
        putBlobs(blobs);
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertStatus(status, status::getCopiedObjectCount).isEqualTo(0);
        assertStatus(status, status::getRemovedObjectCount).isEqualTo(0);
        assertStatus(status, status::getErrorObjectCount).isEqualTo(0);
        assertStatus(status, status::getMovedObjectCount).isEqualTo(0);
        assertStatus(status, status::getLinkedObjectCount).isEqualTo(0);
    }

    @Test
    public void testListLimits() throws Exception {
        String[] blobs = {"a", "b", "c"};
        putBlobs(blobs);
        ArrayList<String> results = new ArrayList<>();
        PageSet<? extends StorageMetadata> listResults = policy.list(containerName, ListContainerOptions.Builder
                .maxResults(2));
        listResults.stream().forEach(meta -> results.add(meta.getName()));
        String marker = listResults.getNextMarker();
        assertThat(marker).isEqualTo("b");
        listResults = policy.list(containerName, ListContainerOptions.Builder.afterMarker(marker));
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
        PageSet<? extends StorageMetadata> listResults = policy.list(containerName, ListContainerOptions.Builder
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
        PageSet<? extends StorageMetadata> listResults = policy.list(containerName, ListContainerOptions.Builder
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
    public void testServerSideCopySource() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        String copyBlobName = blobName + "-copy";
        Blob blob = UtilsTest.makeBlob(policy, blobName, ByteSource.empty());

        policy.getSource().putBlob(containerName, blob);
        policy.copyBlob(containerName, blobName, containerName, copyBlobName, CopyOptions.NONE);
        assertEqualBlobs(policy.getBlob(containerName, copyBlobName), blob);
    }

    @Test
    public void testServerSideCopyDestination() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        String copyBlobName = blobName + "-copy";
        Blob blob = UtilsTest.makeBlob(policy, blobName, ByteSource.empty());

        policy.getDestination().putBlob(containerName, blob);
        policy.copyBlob(containerName, blobName, containerName, copyBlobName,
                CopyOptions.builder().userMetadata(ImmutableMap.of("x", "1")).build());
        assertEqualBlobs(policy.getBlob(containerName, copyBlobName), blob);
    }

    @Test
    public void testLoggingPutGet() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        Blob blob = UtilsTest.makeBlob(policy, blobName);
        policy.putBlob(containerName, blob);
        Blob getBlob = policy.getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(getBlob, blob);
        Queue<StatsQueueEntry> q = app.getBounceStats().getQueue();
        assertThat(q).hasSize(2);
        StatsQueueEntry putEntry = q.remove();
        ArrayList<Object> putOp = putEntry.getValues();
        StatsQueueEntry getEntry = q.remove();
        ArrayList<Object> getOp = getEntry.getValues();
        int blobStoreId = app.getBlobStoreId(((BlobStoreTarget) policy.getDestination()).delegate());
        String destinationStore = ((BlobStoreTarget) policy.getDestination()).mapContainer(null);
        assertThat(putEntry.getDbSeries().getName()).isEqualTo(BounceStats.DBSeries.OPS_SERIES +
                ".provider." + blobStoreId +
                ".container." + destinationStore +
                ".op." + HttpMethod.PUT);
        assertThat(putOp.get(1)).isEqualTo(blobName);
        assertThat(putOp.get(2)).isEqualTo(getBlob.getMetadata().getSize());
        assertThat(getEntry.getDbSeries().getName()).isEqualTo(BounceStats.DBSeries.OPS_SERIES +
                ".provider." + blobStoreId +
                ".container." + destinationStore +
                ".op." + HttpMethod.GET);
        assertThat(getOp.get(1)).isEqualTo(blobName);
        assertThat(getOp.get(2)).isEqualTo(getBlob.getMetadata().getSize());
    }

    private void verifyList(String[] expectedNames) throws Exception {
        List<String> listedBlobNames = policy.list(containerName, new ListContainerOptions().recursive())
                .stream()
                .map(meta -> meta.getName())
                .collect(Collectors.toList());
        assertThat(listedBlobNames).containsExactly(expectedNames);
    }

    private void putBlobs(String[] destinationNames) throws Exception {
        for (String name : destinationNames) {
            Blob blob = UtilsTest.makeBlob(policy, name);
            policy.putBlob(containerName, blob);
        }
    }

    private void putSourceBlobs(String[] sourceNames) throws Exception {
        for (String name : sourceNames) {
            Blob blob = UtilsTest.makeBlob(policy, name);
            policy.getSource().putBlob(containerName, blob);
        }
    }
}
