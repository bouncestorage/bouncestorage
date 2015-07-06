/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static com.bouncestorage.bounce.UtilsTest.assertStatus;
import static com.bouncestorage.bounce.UtilsTest.runBounce;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assume.assumeThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Random;

import javax.ws.rs.HttpMethod;

import com.bouncestorage.bounce.BlobStoreTarget;
import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.BounceApplication;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.bouncestorage.bounce.admin.BounceService;
import com.bouncestorage.bounce.admin.BounceStats;
import com.bouncestorage.bounce.admin.StatsQueueEntry;
import com.bouncestorage.bounce.utils.TestUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.blobstore.options.PutOptions;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteBackPolicyTest {
    String containerName;
    BounceApplication app;
    BouncePolicy policy;
    BounceService bounceService;
    Duration duration = Duration.ofHours(1);
    Logger logger;

    @Before
    public void setUp() throws Exception {
        synchronized (BounceApplication.class) {
            app = new BounceApplication();
        }
        app.useRandomPorts();
        app.registerConfigurationListener();
        app.pauseBackgroundTasks();
        bounceService = new BounceService(app);

        UtilsTest.createTestProvidersConfig(app.getConfiguration());
        containerName = UtilsTest.switchPolicyforContainer(app, WriteBackPolicy.class,
                ImmutableMap.of(WriteBackPolicy.EVICT_DELAY, duration.toString(),
                        WriteBackPolicy.COPY_DELAY, Duration.ofSeconds(0).toString()));
        policy = (BouncePolicy) app.getBlobStore(containerName);

        // need to initialize logger after dropwizard application init
        logger = LoggerFactory.getLogger(WriteBackPolicyTest.class);
    }

    @After
    public void tearDown() {
        if (policy != null) {
            policy.deleteContainer(containerName);
        }

        app.resumeBackgroundTasks();
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
        if (policy.getDestination() instanceof BouncePolicy) {
            assertStatus(status, status::getMovedObjectCount).isEqualTo(2);
        } else {
            assertStatus(status, status::getMovedObjectCount).isEqualTo(1);
        }
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
        Blob blob = UtilsTest.makeBlob(policy, blobName, ByteSource.wrap("blob".getBytes()));
        policy.putBlob(containerName, blob);
        String destinationContainer;
        if (policy.getDestination() instanceof BouncePolicy) {
            destinationContainer = ((BlobStoreTarget) ((BouncePolicy) policy.getDestination()).getSource())
                    .mapContainer(null);
        } else {
            destinationContainer = ((BlobStoreTarget) policy.getDestination()).mapContainer(null);
        }
        assertThat(policy.getDestination().blobExists(destinationContainer, blobName)).isFalse();
        assertThat(policy.getSource().blobExists(containerName, blobName)).isTrue();
        UtilsTest.assertEqualBlobs(policy.getSource().getBlob(containerName, blobName), blob);

        BounceService.BounceTaskStatus status = runBounce(bounceService, containerName);
        if (policy.getDestination() instanceof BouncePolicy) {
            assertStatus(status, status::getCopiedObjectCount).isEqualTo(2);
        } else {
            assertStatus(status, status::getCopiedObjectCount).isEqualTo(1);
        }
        Blob farBlob = policy.getDestination().getBlob(containerName, blobName);
        Blob nearBlob = policy.getSource().getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(farBlob, blob);
        UtilsTest.assertEqualBlobs(nearBlob, blob);
    }

    @Test
    public void testCopiedBlobChangedToLink() throws Exception {
        // Check that after a blob is copied, WriteBack policy will create a link
        String blobName = UtilsTest.createRandomBlobName();
        Blob blob = UtilsTest.makeBlob(policy, blobName);
        policy.putBlob(containerName, blob);

        // Copy the blob over
        BounceService.BounceTaskStatus status = runBounce(bounceService, containerName);
        Blob farBlob = policy.getDestination().getBlob(containerName, blobName);
        Blob nearBlob = policy.getSource().getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(nearBlob, farBlob);

        // Evict the blob and ensure that a link is created
        UtilsTest.advanceServiceClock(app, duration.plusHours(1));
        status = runBounce(bounceService, containerName);
        BlobMetadata source = policy.getSource().blobMetadata(containerName, blobName);
        assertThat(BounceLink.isLink(source)).isTrue();
        assertStatus(status, status::getLinkedObjectCount).isEqualTo(1);
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
        BounceService.BounceTaskStatus status = runBounce(bounceService, containerName);
        Blob farBlob = policy.getDestination().getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(farBlob, blob);
        if (policy.getDestination() instanceof BouncePolicy) {
            assertStatus(status, status::getMovedObjectCount).isEqualTo(2);
        } else {
            assertStatus(status, status::getMovedObjectCount).isEqualTo(1);
        }

        // Run the last modified time policy and ensure that the blob is removed
        policy.removeBlob(containerName, blobName);
        farBlob = policy.getDestination().getBlob(containerName, blobName);
        assertThat(policy.getBlob(containerName, blobName)).isNull();
        UtilsTest.assertEqualBlobs(farBlob, blob);
        status = runBounce(bounceService, containerName);
        if (policy.getDestination() instanceof BouncePolicy) {
            assertStatus(status, status::getRemovedObjectCount).isEqualTo(2);
        } else {
            assertStatus(status, status::getRemovedObjectCount).isEqualTo(1);
        }
        assertThat(policy.getDestination().getBlob(containerName, blobName)).isNull();
    }

    @Test
    public void testOverwriteLink() throws Exception {
        // Check that after moving a blob, a PUT to the same key will force a move
        String blobName = UtilsTest.createRandomBlobName();
        Blob blobFoo = UtilsTest.makeBlob(policy, blobName, ByteSource.wrap("foo".getBytes()));
        Blob blobBar = UtilsTest.makeBlob(policy, blobName, ByteSource.wrap("bar".getBytes()));
        logger.info("PUT foo to {}", containerName);
        policy.putBlob(containerName, blobFoo);

        // Move the blob
        UtilsTest.advanceServiceClock(app, duration.plusHours(1));
        logger.info("BOUNCE foo");
        BounceService.BounceTaskStatus status = runBounce(bounceService, containerName);
        Blob farBlob = policy.getDestination().getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(farBlob, blobFoo);
        if (policy.getDestination() instanceof BouncePolicy) {
            assertStatus(status, status::getMovedObjectCount).isEqualTo(2);
        } else {
            assertStatus(status, status::getMovedObjectCount).isEqualTo(1);
        }

        // Update the object
        logger.info("PUT bar to {}", containerName);
        policy.putBlob(containerName, blobBar);
        Blob nearBlob = policy.getSource().getBlob(containerName, blobName);
        Blob canonicalBlob = policy.getBlob(containerName, blobName);
        farBlob = policy.getDestination().getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(nearBlob, blobBar);
        UtilsTest.assertEqualBlobs(farBlob, blobFoo);
        UtilsTest.assertEqualBlobs(canonicalBlob, blobBar);

        // Run the write back policy and ensure that the blob is updated
        logger.info("BOUNCE bar");
        UtilsTest.advanceServiceClock(app, duration.plusHours(1));
        status = runBounce(bounceService, containerName);
        if (policy.getDestination() instanceof BouncePolicy) {
            assertStatus(status, status::getMovedObjectCount).isEqualTo(2);
        } else {
            assertStatus(status, status::getMovedObjectCount).isEqualTo(1);
        }
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
        BounceService.BounceTaskStatus status = runBounce(bounceService, containerName);
        assertStatus(status, status::getTotalObjectCount).isNotEqualTo(0);
        Blob farBlob = policy.getDestination().getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(farBlob, blobFoo);
        if (policy.getDestination() instanceof BouncePolicy) {
            assertStatus(status, status::getCopiedObjectCount).isEqualTo(2);
        } else {
            assertStatus(status, status::getCopiedObjectCount).isEqualTo(1);
        }

        // Update the object
        policy.putBlob(containerName, blobBar);
        Blob nearBlob = policy.getSource().getBlob(containerName, blobName);
        Blob canonicalBlob = policy.getBlob(containerName, blobName);
        farBlob = policy.getDestination().getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(nearBlob, blobBar);
        UtilsTest.assertEqualBlobs(farBlob, blobFoo);
        UtilsTest.assertEqualBlobs(canonicalBlob, blobBar);

        // Run the write back policy and ensure that the blob is updated
        status = runBounce(bounceService, containerName);
        if (policy.getDestination() instanceof BouncePolicy) {
            assertStatus(status, status::getCopiedObjectCount).isEqualTo(2);
        } else {
            assertStatus(status, status::getCopiedObjectCount).isEqualTo(1);
        }
        farBlob = policy.getDestination().getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(farBlob, blobBar);
    }

    @Test
    public void testCopyAndThenBounce() throws Exception {
        String fromBlobName = UtilsTest.createRandomBlobName();
        String toBlobName = UtilsTest.createRandomBlobName();
        Blob blob = UtilsTest.makeBlob(policy, fromBlobName, ByteSource.wrap("foo".getBytes()));
        policy.putBlob(containerName, blob);
        policy.copyBlob(containerName, fromBlobName, containerName, toBlobName, CopyOptions.NONE);

        runBounce(bounceService, containerName);

        BlobMetadata meta = policy.getDestination().blobMetadata(containerName, fromBlobName);
        assertThat(BounceLink.isLink(meta)).isFalse();
        meta = policy.getDestination().blobMetadata(containerName, toBlobName);
        assertThat(BounceLink.isLink(meta)).isFalse();
        meta = policy.getSource().blobMetadata(containerName, fromBlobName);
        assertThat(BounceLink.isLink(meta)).isFalse();
        meta = policy.getSource().blobMetadata(containerName, toBlobName);
        assertThat(BounceLink.isLink(meta)).isFalse();
    }

    @Test
    public void testCopyAfterCopyBounce() throws Exception {
        String fromBlobName = UtilsTest.createRandomBlobName();
        String toBlobName = UtilsTest.createRandomBlobName();
        Blob blob = UtilsTest.makeBlob(policy, fromBlobName, ByteSource.wrap("foo".getBytes()));
        policy.putBlob(containerName, blob);

        runBounce(bounceService, containerName);

        String etag = policy.copyBlob(containerName, fromBlobName, containerName, toBlobName, CopyOptions.NONE);
        assertThat(etag).isNotNull();
        Blob linkedBlob = policy.getBlob(containerName, toBlobName);
        UtilsTest.assertEqualBlobs(linkedBlob, blob);
    }

    @Test
    public void testCopyAfterMoveBounce() throws Exception {
        String fromBlobName = UtilsTest.createRandomBlobName();
        String toBlobName = UtilsTest.createRandomBlobName();
        Blob blob = UtilsTest.makeBlob(policy, fromBlobName, ByteSource.wrap("foo".getBytes()));
        policy.putBlob(containerName, blob);

        UtilsTest.advanceServiceClock(app, duration.plusHours(1));
        BounceService.BounceTaskStatus status = runBounce(bounceService, containerName);
        assertStatus(status, status::getMovedObjectCount).isNotEqualTo(0);

        String etag = policy.copyBlob(containerName, fromBlobName, containerName, toBlobName, CopyOptions.NONE);
        assertThat(etag).isNotNull();
        Blob linkedBlob = policy.getBlob(containerName, toBlobName);
        UtilsTest.assertEqualBlobs(linkedBlob, blob);
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
                    Blob blob = UtilsTest.makeBlob(b, blobName, TestUtils.randomByteSource().slice(0, blobLen));
                    b.putBlob(containerName, blob);
                });
                numBlobsInStore++;
            } else if (op == 1) {
                // GET
                Blob one = reference.getBlob(containerName, blobName);
                Blob two = policy.getBlob(containerName, blobName);
                UtilsTest.assertEqualBlobs(two, one);
            } else if (op == 2) {
                // DELETE
                blobStores.forEach(b -> b.removeBlob(containerName, blobName));
            }
        }

        assertEqualBlobStores(reference, policy);
        BounceService.BounceTaskStatus status = runBounce(bounceService, containerName);
        if (policy.getDestination() instanceof BouncePolicy) {
            assertStatus(status, status::getTotalObjectCount).isLessThanOrEqualTo(numBlobsInStore * 2);
        } else {
            assertStatus(status, status::getTotalObjectCount).isLessThanOrEqualTo(numBlobsInStore);
        }
        numBlobsInStore = (int) status.getTotalObjectCount();
        assertStatus(status, status::getCopiedObjectCount).isEqualTo(numBlobsInStore);
        assertEqualBlobStores(reference, policy);

        UtilsTest.advanceServiceClock(app, duration.plusSeconds(1));
        status = runBounce(bounceService, containerName);
        assertStatus(status, status::getTotalObjectCount).isEqualTo(numBlobsInStore);
        assertThat(status.getLinkedObjectCount() + status.getMovedObjectCount()).isEqualTo(numBlobsInStore);
    }

    @Test
    public void testPutGetLogging() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        Blob blob = UtilsTest.makeBlob(policy, blobName);
        policy.putBlob(containerName, blob);
        Blob getBlob = policy.getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(getBlob, blob);
        Queue<StatsQueueEntry> q = app.getBounceStats().getQueue();
        assertThat(q).hasSize(3);
        StatsQueueEntry putMarkerEntry = q.remove();
        ArrayList<Object> putMarkerOp = putMarkerEntry.getValues();
        StatsQueueEntry putEntry = q.remove();
        ArrayList<Object> putOp = putEntry.getValues();
        StatsQueueEntry getEntry = q.remove();
        ArrayList<Object> getOp = getEntry.getValues();
        int blobStoreId = app.getBlobStoreId(((BlobStoreTarget) policy.getSource()).delegate());
        String namePrefix = BounceStats.DBSeries.OPS_SERIES +
                ".provider." + blobStoreId +
                ".container." + containerName +
                ".op.";
        assertThat(putMarkerEntry.getDbSeries().getName()).isEqualTo(namePrefix + HttpMethod.PUT);
        assertThat(putMarkerOp.get(1)).isEqualTo(blobName + WriteBackPolicy.LOG_MARKER_SUFFIX);
        assertThat(putEntry.getDbSeries().getName()).isEqualTo(namePrefix + HttpMethod.PUT);
        assertThat(putOp.get(1)).isEqualTo(blobName);
        assertThat(putOp.get(2)).isEqualTo(getBlob.getMetadata().getSize());
        assertThat(getEntry.getDbSeries().getName()).isEqualTo(namePrefix + HttpMethod.GET);
        assertThat(getOp.get(1)).isEqualTo(blobName);
        assertThat(getOp.get(2)).isEqualTo(getBlob.getMetadata().getSize());
    }

    @Test
    public void testPutLargeObject() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        final long size = 5L * 1024 * 1024 * 1024 /* 5GB */ + 1;
        putLargeObject(blobName, size);
    }

    private void skipIfTransient(BlobStore blobStore) {
        if (blobStore instanceof BouncePolicy) {
            BouncePolicy bouncePolicy = (BouncePolicy) blobStore;
            skipIfTransient(bouncePolicy.getSource());
            skipIfTransient(bouncePolicy.getDestination());
        } else {
            assumeThat(blobStore.getContext().unwrap().getId(), not(is("transient")));
        }
    }

    private void putLargeObject(String blobName, long size) throws Exception {
        /* running 5GB through transient blob store is asking for trouble */
        skipIfTransient(policy);

        Blob blob = policy.blobBuilder(blobName)
                .payload(TestUtils.randomByteSource().slice(0, size))
                .contentLength(size)
                .build();

        logger.info("putting large object {}", blobName);
        policy.putBlob(containerName, blob, new PutOptions().multipart());
        logger.info("done putting large object {}", blobName);
        assertThat(policy.getDestination().blobExists(containerName, blobName)).isFalse();
        assertThat(policy.getSource().blobExists(containerName, blobName)).isTrue();
        BlobMetadata nearBlob = policy.getSource().blobMetadata(containerName, blobName);
        assertThat(nearBlob.getContentMetadata().getContentLength()).isEqualTo(size);
    }

    @Test
    public void testBounceLargeObject() throws Exception {
        skipIfTransient(policy);

        String blobName = UtilsTest.createRandomBlobName();
        final long size = 5L * 1024 * 1024 * 1024 /* 5GB */ + 1;
        putLargeObject(blobName, size);

        logger.info("bouncing {}", blobName);
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertStatus(status, status::getErrorObjectCount).isEqualTo(0);
        logger.info("done bouncing {}", blobName);

        BlobMetadata farBlob = policy.getDestination().blobMetadata(containerName, blobName);
        BlobMetadata nearBlob = policy.getSource().blobMetadata(containerName, blobName);
        assertThat(farBlob.getContentMetadata().getContentLength()).isEqualTo(size);
        assertThat(nearBlob.getContentMetadata().getContentLength()).isEqualTo(size);
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
