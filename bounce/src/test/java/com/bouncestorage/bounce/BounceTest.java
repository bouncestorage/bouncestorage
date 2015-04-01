/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import static org.assertj.core.api.Assertions.assertThat;

import com.bouncestorage.bounce.admin.BouncePolicy;
import com.bouncestorage.bounce.admin.policy.MoveEverythingPolicy;
import com.bouncestorage.bounce.admin.policy.WriteBackPolicy;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.io.ContentMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class BounceTest {
    private BlobStoreContext bounceContext;
    private BlobStore nearBlobStore;
    private BlobStore farBlobStore;
    private BouncePolicy policy;
    private String containerName;

    @Before
    public void setUp() throws Exception {
        containerName = UtilsTest.createRandomContainerName();

        policy = new WriteBackPolicy();
        policy.setBlobStores(UtilsTest.createTransientBlobStore(), UtilsTest.createTransientBlobStore());

        nearBlobStore = policy.getSource();
        farBlobStore = policy.getDestination();
        policy.createContainerInLocation(null, containerName);
    }

    @After
    public void tearDown() throws Exception {
        if (policy != null) {
            policy.deleteContainer(containerName);
        }
        if (bounceContext != null) {
            bounceContext.close();
        }
    }

    @Test
    public void testCreateBucket() throws Exception {
        assertThat(nearBlobStore.containerExists(containerName)).isTrue();
        assertThat(farBlobStore.containerExists(containerName)).isTrue();
    }

    @Test
    public void testCreateLink() throws Exception {
        String blobName = "blob";
        ByteSource byteSource = ByteSource.wrap(new byte[1]);
        Blob blob = UtilsTest.makeBlob(nearBlobStore, blobName, byteSource);
        nearBlobStore.putBlob(containerName, blob);

        assertThat(BounceLink.isLink(nearBlobStore.blobMetadata(
                containerName, blobName))).isFalse();

        Utils.copyBlobAndCreateBounceLink(nearBlobStore, farBlobStore,
                containerName, blobName);

        assertThat(BounceLink.isLink(nearBlobStore.blobMetadata(
                containerName, blobName))).isTrue();
    }

    @Test
    public void testBounceNonexistentBlob() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        Utils.copyBlobAndCreateBounceLink(nearBlobStore, farBlobStore,
                containerName, blobName);
        assertThat(nearBlobStore.blobExists(containerName, blobName)).isFalse();
        assertThat(farBlobStore.blobExists(containerName, blobName)).isFalse();
    }

    @Test
    public void test404Meta() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        BlobMetadata meta = policy.blobMetadata(containerName, blobName);
        assertThat(meta).isNull();
    }

    @Test
    public void testBounceBlob() throws Exception {
        String blobName = "blob";
        ByteSource byteSource = ByteSource.wrap(new byte[1]);
        Blob blob = UtilsTest.makeBlob(policy, blobName, byteSource);
        ContentMetadata metadata = blob.getMetadata().getContentMetadata();
        policy.putBlob(containerName, blob);
        BlobMetadata meta1 = policy.blobMetadata(containerName, blobName);

        Utils.copyBlobAndCreateBounceLink(nearBlobStore, farBlobStore,
                containerName, blobName);
        BlobMetadata meta2 = policy.blobMetadata(containerName, blobName);
        assertThat((Object) meta2).isEqualToComparingFieldByField(meta1);

        Blob blob2 = policy.getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(blob, blob2);
    }

    @Test
    public void testTakeOverFarStore() throws Exception {
        String blobName = "blob";
        Blob blob = UtilsTest.makeBlob(farBlobStore, blobName);
        farBlobStore.putBlob(containerName, blob);
        assertThat(nearBlobStore.blobExists(containerName, blobName)).isFalse();
        assertThat(policy.sanityCheck(containerName)).isFalse();

        policy.takeOver(containerName);
        assertThat(nearBlobStore.blobExists(containerName, blobName)).isTrue();
        assertThat(BounceLink.isLink(nearBlobStore.blobMetadata(
                containerName, blobName))).isTrue();
        assertThat(policy.sanityCheck(containerName)).isTrue();
    }

    @Test
    public void testUnbounce() throws Exception {
        String blobName = "blob";
        Blob blob = UtilsTest.makeBlob(farBlobStore, blobName);
        nearBlobStore.putBlob(containerName, blob);
        Utils.copyBlobAndCreateBounceLink(nearBlobStore, farBlobStore,
                containerName, blobName);

        assertThat(nearBlobStore.blobExists(containerName, blobName)).isTrue();
        assertThat(farBlobStore.blobExists(containerName, blobName)).isTrue();
        Blob nearBlob = nearBlobStore.getBlob(containerName, blobName);
        assertThat(BounceLink.isLink(nearBlob.getMetadata())).isTrue();

        policy.getBlob(containerName, blobName, GetOptions.NONE);
        assertThat(nearBlobStore.blobExists(containerName, blobName)).isTrue();
        assertThat(farBlobStore.blobExists(containerName, blobName)).isTrue();
        nearBlob = nearBlobStore.getBlob(containerName, blobName);
        assertThat(BounceLink.isLink(nearBlob.getMetadata())).isFalse();
        Blob farBlob = farBlobStore.getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(nearBlob, blob);
        UtilsTest.assertEqualBlobs(farBlob, blob);
    }

    @Test
    public void testCopyBlob() throws Exception {
        String blobName = "blob";
        Blob blob = UtilsTest.makeBlob(nearBlobStore, blobName);
        nearBlobStore.putBlob(containerName, blob);
        assertThat(nearBlobStore.blobExists(containerName, blobName)).isTrue();
        Utils.copyBlob(nearBlobStore, farBlobStore, containerName, containerName, blobName);
        Blob farBlob = farBlobStore.getBlob(containerName, blobName);
        UtilsTest.assertEqualBlobs(farBlob, blob);
    }

    @Test
    public void testUpdateMetadataTest() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        Blob blob = UtilsTest.makeBlob(policy, blobName);
        blob.getMetadata().setUserMetadata(ImmutableMap.of("foo", "1"));
        policy.putBlob(containerName, blob);
        assertThat(policy.blobMetadata(containerName, blobName).getUserMetadata())
                .containsKey("foo")
                .doesNotContainKey("bar");
        policy.updateBlobMetadata(containerName, blobName, ImmutableMap.of("bar", "2"));
        assertThat(policy.blobMetadata(containerName, blobName).getUserMetadata()).containsKeys("foo", "bar");
    }
}
