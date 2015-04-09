/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.BounceApplication;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.bouncestorage.bounce.admin.BounceService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class WriteBackPolicyTest {
    String containerName;
    BounceApplication app;
    BouncePolicy policy;
    BounceService bounceService;
    Duration duration = Duration.ofHours(1);
    Logger logger = LoggerFactory.getLogger(WriteBackPolicyTest.class);

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
        UtilsTest.switchPolicyforContainer(app, containerName, WriteBackPolicy.class,
                ImmutableMap.of(WriteBackPolicy.EVICT_DELAY, duration.toString(),
                        WriteBackPolicy.COPY_DELAY, Duration.ofSeconds(0).toString()));
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
    public void testMoveObject() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        Blob blob = UtilsTest.makeBlob(policy, blobName);
        policy.putBlob(containerName, blob);
        assertThat(policy.getDestination().blobExists(containerName, blobName)).isFalse();
        assertThat(policy.getSource().blobExists(containerName, blobName)).isTrue();

        UtilsTest.advanceServiceClock(app, duration.plusHours(1));
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getMovedObjectCount()).isEqualTo(1);
        assertThat(policy.getDestination().blobExists(containerName, blobName)).isTrue();
        assertThat(policy.getSource().blobExists(containerName, blobName)).isTrue();
        BlobMetadata source = policy.getSource().blobMetadata(containerName, blobName);
        assertThat(BounceLink.isLink(source)).isTrue();
        Blob linkedBlob = policy.getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(linkedBlob, blob);
    }

    @Test
    public void testCopyObject() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        Blob blob = UtilsTest.makeBlob(policy, blobName);
        policy.putBlob(containerName, blob);
        assertThat(policy.getDestination().blobExists(containerName, blobName)).isFalse();
        assertThat(policy.getSource().blobExists(containerName, blobName)).isTrue();

        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getCopiedObjectCount()).isEqualTo(1);
        Blob farBlob = policy.getDestination().getBlob(containerName, blobName);
        Blob nearBlob = policy.getSource().getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(blob, farBlob);
        UtilsTest.assertEqualBlobs(blob, nearBlob);
    }

    @Test
    public void testCopiedBlobChangedToLink() throws Exception {
        // Check that after a blob is copied, WriteBack policy will create a link
        String blobName = UtilsTest.createRandomBlobName();
        Blob blob = UtilsTest.makeBlob(policy, blobName);
        policy.putBlob(containerName, blob);

        // Copy the blob over
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        Blob farBlob = policy.getDestination().getBlob(containerName, blobName);
        Blob nearBlob = policy.getSource().getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(nearBlob, farBlob);

        // Evict the blob and ensure that a link is created
        UtilsTest.advanceServiceClock(app, duration.plusHours(1));
        status = bounceService.bounce(containerName);
        status.future().get();
        BlobMetadata source = policy.getSource().blobMetadata(containerName, blobName);
        assertThat(BounceLink.isLink(source)).isTrue();
        assertThat(status.getLinkedObjectCount()).isEqualTo(1);
        Blob linkedBlob = policy.getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(linkedBlob, blob);
    }

    @Test
    public void testRemoveFarBlobWithoutLink() throws Exception {
        // Check that after a link is removed, the blob in the far store is removed as well
        String blobName = UtilsTest.createRandomBlobName();
        Blob blob = UtilsTest.makeBlob(policy, blobName);
        policy.putBlob(containerName, blob);

        // Move the blob over
        UtilsTest.advanceServiceClock(app, duration.plusHours(1));
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        Blob farBlob = policy.getDestination().getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(blob, farBlob);
        assertThat(status.getMovedObjectCount()).isEqualTo(1);

        // Run the last modified time policy and ensure that the blob is removed
        policy.removeBlob(containerName, blobName);
        farBlob = policy.getDestination().getBlob(containerName, blobName);
        assertThat(policy.getBlob(containerName, blobName)).isNull();
        UtilsTest.assertEqualBlobs(farBlob, blob);
        status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getRemovedObjectCount()).isEqualTo(1);
        assertThat(policy.getDestination().getBlob(containerName, blobName)).isNull();
    }

    @Test
    public void testOverwriteLink() throws Exception {
        // Check that after moving a blob, a PUT to the same key will force a move
        String blobName = UtilsTest.createRandomBlobName();
        Blob blobFoo = UtilsTest.makeBlob(policy, blobName, ByteSource.wrap("foo".getBytes()));
        Blob blobBar = UtilsTest.makeBlob(policy, blobName, ByteSource.wrap("bar".getBytes()));
        policy.putBlob(containerName, blobFoo);

        // Move the blob
        UtilsTest.advanceServiceClock(app, duration.plusHours(1));
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        Blob farBlob = policy.getDestination().getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(blobFoo, farBlob);
        assertThat(status.getMovedObjectCount()).isEqualTo(1);

        // Update the object
        policy.putBlob(containerName, blobBar);
        Blob nearBlob = policy.getSource().getBlob(containerName, blobName);
        Blob canonicalBlob = policy.getBlob(containerName, blobName);
        farBlob = policy.getDestination().getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(nearBlob, blobBar);
        UtilsTest.assertEqualBlobs(farBlob, blobFoo);
        UtilsTest.assertEqualBlobs(canonicalBlob, blobBar);

        // Run the write back policy and ensure that the blob is updated
        UtilsTest.advanceServiceClock(app, duration.plusHours(1));
        status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getMovedObjectCount()).isEqualTo(1);
        farBlob = policy.getDestination().getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(farBlob, blobBar);
    }

    @Test
    public void testCopyAfterBlobUpdate() throws Exception {
        // Tests that after updating the blob, we will copy it over again
        String blobName = UtilsTest.createRandomBlobName();
        Blob blobFoo = UtilsTest.makeBlob(policy, blobName, ByteSource.wrap("foo".getBytes()));
        Blob blobBar = UtilsTest.makeBlob(policy, blobName, ByteSource.wrap("bar".getBytes()));
        policy.putBlob(containerName, blobFoo);

        // Copy the blob
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        Blob farBlob = policy.getDestination().getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(blobFoo, farBlob);
        assertThat(status.getCopiedObjectCount()).isEqualTo(1);

        // Update the object
        policy.putBlob(containerName, blobBar);
        Blob nearBlob = policy.getSource().getBlob(containerName, blobName);
        Blob canonicalBlob = policy.getBlob(containerName, blobName);
        farBlob = policy.getDestination().getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(nearBlob, blobBar);
        UtilsTest.assertEqualBlobs(farBlob, blobFoo);
        UtilsTest.assertEqualBlobs(canonicalBlob, blobBar);

        // Run the write back policy and ensure that the blob is updated
        status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getCopiedObjectCount()).isEqualTo(1);
        farBlob = policy.getDestination().getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(farBlob, blobBar);
    }

    @Test
    public void testRandomOperations() throws Exception {
        BlobStore reference = UtilsTest.createTransientBlobStore();
        reference.createContainerInLocation(null, containerName);
        final int baseNumTest = 100;
        Random r = new Random();
        if (System.getProperty("TEST_SEED") != null) {
            r.setSeed(Long.valueOf(System.getProperty("TEST_SEED")));
        } else {
            long seed = new Random().nextLong();
            logger.info("seed: {}", seed);
            r.setSeed(seed);
        }

        int objectCount = baseNumTest + r.nextInt(baseNumTest);
        ArrayList<String> blobNames = new ArrayList<>(objectCount);
        for (int i = 0; i < objectCount; i++) {
            blobNames.add("blob-" + r.nextInt(Integer.MAX_VALUE));
        }

        List<BlobStore> blobStores = ImmutableList.of(reference, policy);
        int numBlobsInStore = 0;

        int opsCount = baseNumTest + r.nextInt(baseNumTest);
        for (int i = 0; i < opsCount; i++) {
            String blobName = blobNames.get(r.nextInt(blobNames.size()));
            int op = r.nextInt(3);
            if (op == 0) {
                // PUT
                int blobLen = 1000 + r.nextInt(1000);
                blobStores.forEach(b -> {
                    Blob blob = UtilsTest.makeBlob(b, blobName, ByteSource.wrap(new byte[blobLen]));
                    b.putBlob(containerName, blob);
                });
                numBlobsInStore++;
            } else if (op == 1) {
                // GET
                Blob one = reference.getBlob(containerName, blobName);
                Blob two = policy.getBlob(containerName, blobName);
                UtilsTest.assertEqualBlobs(one, two);
            } else if (op == 2) {
                // DELETE
                blobStores.forEach(b -> b.removeBlob(containerName, blobName));
            }
        }

        assertEqualBlobStores(reference, policy);
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getTotalObjectCount()).isLessThanOrEqualTo(numBlobsInStore);
        numBlobsInStore = (int) status.getTotalObjectCount();
        assertThat(status.getCopiedObjectCount()).isEqualTo(numBlobsInStore);
        assertEqualBlobStores(reference, policy);

        UtilsTest.advanceServiceClock(app, duration.plusSeconds(1));
        status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getTotalObjectCount()).isEqualTo(numBlobsInStore);
        assertThat(status.getLinkedObjectCount()).isEqualTo(numBlobsInStore);
    }

    private void assertEqualBlobStores(BlobStore one, BlobStore two) throws Exception {
        Iterator<StorageMetadata> iterFromOne = Utils.crawlBlobStore(one, containerName).iterator();
        Iterator<StorageMetadata> iterFromTwo = Utils.crawlBlobStore(two, containerName).iterator();

        while (iterFromOne.hasNext() && iterFromTwo.hasNext()) {
            StorageMetadata refMeta = iterFromOne.next();
            StorageMetadata policyMeta = iterFromTwo.next();
            assertThat(Utils.equalsOtherThanTime(refMeta, policyMeta)).isTrue();
            Blob blobOne = one.getBlob(containerName, refMeta.getName());
            Blob blobTwo = two.getBlob(containerName, refMeta.getName());
            UtilsTest.assertEqualBlobs(blobOne, blobTwo);
        }

        assertThat(iterFromOne.hasNext()).isEqualTo(iterFromTwo.hasNext());
    }
}
