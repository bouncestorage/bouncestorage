/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;
import java.util.TreeMap;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.policy.CopyPolicy;
import com.bouncestorage.bounce.admin.policy.MarkerPolicy;
import com.bouncestorage.bounce.admin.policy.MoveEverythingPolicy;
import com.bouncestorage.bounce.admin.policy.MovePolicy;

import org.apache.commons.configuration.MapConfiguration;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FsckTest {
    public static final String RANDOM_SEED = "bounce.test.randomSeed";
    private Logger logger = LoggerFactory.getLogger(getClass());
    private BlobStoreContext bounceContext;
    private BounceBlobStore blobStore;
    private String containerName;
    private BounceService bounceService;

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
    }

    @After
    public void tearDown() throws Exception {
        if (blobStore != null) {
            blobStore.deleteContainer(containerName);
        }
        if (bounceContext != null) {
            bounceContext.close();
        }
    }

    @Test
    public void testNothingToFsck() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        blobStore.putBlob(containerName, UtilsTest.makeBlob(blobStore, blobName));
        BounceService.BounceTaskStatus status = bounceService.fsck(containerName);
        status.future().get();
        fsckNoOp(1);
    }

    @Test
    public void testNothingToFsckLink() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        blobStore.putBlob(containerName, UtilsTest.makeBlob(blobStore, blobName));
        blobStore.copyBlobAndCreateBounceLink(containerName, blobName);
        BounceService.BounceTaskStatus status = bounceService.fsck(containerName);
        status.future().get();
        fsckNoOp(1);
    }

    @Test
    public void testLeakedObjects() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        blobStore.putBlob(containerName, UtilsTest.makeBlob(blobStore, blobName));
        blobStore.copyBlobAndCreateBounceLink(containerName, blobName);
        blobStore.removeBlob(containerName, blobName);
        BounceService.BounceTaskStatus status = bounceService.fsck(containerName);
        status.future().get();
        assertThat(status.totalObjectCount.get()).as("total").isEqualTo(0);
        assertThat(status.removedObjectCount.get()).as("removed").isEqualTo(1);
        assertThat(status.copiedObjectCount.get()).as("copied").isEqualTo(0);
        assertThat(status.movedObjectCount.get()).as("moved").isEqualTo(0);
        fsckNoOp(0);
    }

    @Test
    public void testStaleObjects() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        blobStore.setPolicy(new MoveEverythingPolicy());
        blobStore.putBlob(containerName, UtilsTest.makeBlobRandomSize(blobStore, blobName));
        blobStore.copyBlobAndCreateBounceLink(containerName, blobName);
        blobStore.putBlob(containerName, UtilsTest.makeBlobRandomSize(blobStore, blobName));
        BounceService.BounceTaskStatus status = bounceService.fsck(containerName);
        status.future().get();
        assertThat(status.totalObjectCount.get()).as("total").isEqualTo(1);
        assertThat(status.removedObjectCount.get()).as("removed").isEqualTo(1);
        assertThat(status.copiedObjectCount.get()).as("copied").isEqualTo(0);
        assertThat(status.movedObjectCount.get()).as("moved").isEqualTo(0);
        fsckNoOp(1);
    }

    @Test
    public void randomTest() throws Exception {
        final Random r = new Random();
        long seed;
        if (System.getProperty(RANDOM_SEED) != null) {
            seed = Long.parseLong(System.getProperty(RANDOM_SEED));
        } else {
            seed = r.nextLong();
        }
        r.setSeed(seed);
        logger.info("seed is {}", seed);
        int numBlobs = r.nextInt(100) + 10;
        logger.info("populating {} blobs", numBlobs);
        TreeMap<String, String> blobs = new TreeMap<>();
        for (int i = 0; i < numBlobs; i++) {
            String name = UtilsTest.createRandomBlobName();
            Blob b = UtilsTest.makeBlobRandomSize(blobStore, name);
            String etag = blobStore.putBlob(containerName, b);
            blobs.put(name, etag);
        }

        BouncePolicy policy = new BouncePolicy() {
            private BlobStore nearStore;
            private BlobStore farStore;

            @Override
            public BounceResult bounce(BounceBlobStore bounceBlobStore, String container, BounceStorageMetadata meta)
                    throws
                    IOException {
                boolean copy = r.nextBoolean();
                BounceResult res = copy ? CopyPolicy.copyBounce(bounceBlobStore, container, meta) :
                        MovePolicy.moveBounce(bounceBlobStore, container, meta);
                logger.info("{} {} {}", copy ? "copied" : "moved", meta.getName(), res);
                return res;
            }

            @Override
            public boolean test(StorageMetadata metadata) {
                return r.nextBoolean();
            }

            @Override
            public BounceResult reconcile(String container, BounceStorageMetadata metadata) {
                return BounceResult.NO_OP;
            }

            @Override
            public void setBlobStores(BlobStore source, BlobStore destination) {
                nearStore = source;
                farStore = destination;
            }

            @Override
            public Blob getBlob(String container, String blobName, GetOptions options) {
                return null;
            }

            @Override
            public String putBlob(String container, Blob blob, PutOptions options) {
                return null;
            }

            @Override
            public BlobStore getSource() {
                return nearStore;
            }

            @Override
            public BlobStore getDestination() {
                return farStore;
            }
        };

        bounceService.setDefaultPolicy(policy);

        int numBounces = r.nextInt(10);
        for (int i = 0; i < numBounces; i++) {
            logger.info("bouncing");
            BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
            status.future().get();
        }

        int count = 0;
        for (StorageMetadata meta : Utils.crawlBlobStore(blobStore, containerName)) {
            count++;
            String name = meta.getName();
            String expectEtag = blobs.get(name);
            assertThat(meta.getETag()).as("%s etag", name).isEqualTo(expectEtag);
            BounceStorageMetadata bounceMeta = (BounceStorageMetadata) meta;
            Blob b = blobStore.getFromNearStore(containerName, name);
            if (bounceMeta.getRegions().contains(BounceBlobStore.Region.NEAR)) {
                assertThat(BounceLink.isLink(b.getMetadata())).isFalse();
            } else {
                assertThat(BounceLink.isLink(b.getMetadata())).isTrue();
            }
            b = blobStore.getFromFarStore(containerName, name);
            if (bounceMeta.getRegions().contains(BounceBlobStore.Region.FAR)) {
                assertThat(b.getMetadata().getETag()).isEqualTo(expectEtag);
                assertThat(blobStore.getFromNearStore(containerName,
                        name + MarkerPolicy.LOG_MARKER_SUFFIX)).isNull();
            } else {
                assertThat(b).isNull();
            }
        }

        assertThat(count).as("count").isEqualTo(blobs.size());

        fsckNoOp(numBlobs);
    }

    private void fsckNoOp(int expectTotal) throws Exception {
        BounceService.BounceTaskStatus status = bounceService.fsck(containerName);
        status.future().get();
        assertThat(status.totalObjectCount.get()).as("total").isEqualTo(expectTotal);
        assertThat(status.removedObjectCount.get()).as("removed").isEqualTo(0);
        assertThat(status.copiedObjectCount.get()).as("copied").isEqualTo(0);
        assertThat(status.movedObjectCount.get()).as("moved").isEqualTo(0);
    }
}
