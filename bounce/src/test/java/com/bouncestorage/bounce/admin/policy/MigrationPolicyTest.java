/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

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

import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.BounceApplication;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.bouncestorage.bounce.admin.BounceService;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
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
        containerName = UtilsTest.createRandomContainerName();

        synchronized (BounceApplication.class) {
            app = new BounceApplication();
        }
        app.useRandomPorts();
        app.registerConfigurationListener();
        bounceService = new BounceService(app);

        UtilsTest.createTestProvidersConfig(app.getConfiguration());
        UtilsTest.switchPolicyforContainer(app, containerName, MigrationPolicy.class);
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
    public void testLoggingPutGet() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        Blob blob = UtilsTest.makeBlob(policy, blobName);
        policy.putBlob(containerName, blob);
        Blob getBlob = policy.getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(getBlob, blob);
        Queue<Object[]> q = app.getBounceStats().getOpsQueue();
        assertThat(q).hasSize(2);
        Object[] putOp = q.remove();
        Object[] getOp = q.remove();
        assertThat(putOp[1]).isEqualTo(HttpMethod.PUT);
        assertThat(putOp[2]).isEqualTo(containerName + "-dest");
        assertThat(putOp[3]).isEqualTo(blobName);
        assertThat(putOp[4]).isEqualTo(getBlob.getMetadata().getSize());
        assertThat(getOp[1]).isEqualTo(HttpMethod.GET);
        assertThat(getOp[2]).isEqualTo(containerName + "-dest");
        assertThat(getOp[3]).isEqualTo(blobName);
        assertThat(getOp[4]).isEqualTo(getBlob.getMetadata().getSize());
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
