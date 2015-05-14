/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static com.bouncestorage.bounce.UtilsTest.assertStatus;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import com.bouncestorage.bounce.BlobStoreTarget;
import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.bouncestorage.bounce.admin.BounceService;
import com.bouncestorage.bounce.admin.ConfigurationResource;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.junit.Before;
import org.junit.Test;

public final class ThreeTierWriteBackPolicyTest extends WriteBackPolicyTest {
    @Before
    public void setUp() throws Exception {
        super.setUp();

        Properties p = new Properties();
        p.put("bounce.backend.2.jclouds.provider", "transient");
        p.put("bounce.backends", "0,1,2");
        new ConfigurationResource(app).updateConfig(p);
        String prefix = "bounce.container.0.";
        Map<Object, Object> m = ImmutableMap.builder()
                .put(prefix + "name", containerName)
                .put(prefix + "identity", "identity")
                .put(prefix + "credential", "credential")
                .put(prefix + "tier.1.policy", WriteBackPolicy.class.getSimpleName())
                .put(prefix + "tier.1.copyDelay", Duration.ofHours(0).toString())
                .put(prefix + "tier.1.evictDelay", Duration.ofHours(1).toString())
                .put(prefix + "tier.2.backend", "2")
                .build();
        p = new Properties();
        p.putAll(m);
        new ConfigurationResource(app).updateConfig(p);
        Map.Entry<String, BlobStore> entry = app.locateBlobStore("identity", containerName, null);
        assertThat(entry).isNotNull();
        assertThat(entry.getKey()).isEqualTo("credential");
        assertThat(entry.getValue()).isInstanceOf(BouncePolicy.class);
        policy = (BouncePolicy) entry.getValue();
        assertThat(policy.getSource()).isInstanceOf(BlobStoreTarget.class);
        assertThat(policy.getDestination()).isInstanceOf(BouncePolicy.class);
        policy = (BouncePolicy) policy.getDestination();
        assertThat(policy.getSource()).isInstanceOf(BlobStoreTarget.class);
        assertThat(policy.getDestination()).isInstanceOf(BlobStoreTarget.class);
        policy.getDestination().createContainerInLocation(null, containerName);
        policy = (BouncePolicy) entry.getValue();
    }

    @Test
    public void testListThreeTiers() throws Exception {
        BlobStore blobStore = app.getBlobStore(0);
        String cacheBlobName = UtilsTest.createRandomBlobName();
        Blob cacheBlob = UtilsTest.makeBlob(blobStore, cacheBlobName);
        blobStore.putBlob(containerName, cacheBlob);
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertStatus(status, status::getCopiedObjectCount).isEqualTo(2);

        StorageMetadata metadata = policy.list(containerName).iterator().next();
        assertThat(metadata).isInstanceOf(BounceStorageMetadata.class);
        assertThat(((BounceStorageMetadata) metadata).getRegions()).isEqualTo(ImmutableSet.of(
                BounceStorageMetadata.Region.FAR,
                BounceStorageMetadata.Region.NEAR,
                BounceStorageMetadata.Region.FARTHER));
    }
}
