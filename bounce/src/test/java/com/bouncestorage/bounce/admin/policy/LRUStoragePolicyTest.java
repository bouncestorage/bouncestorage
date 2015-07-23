/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static com.bouncestorage.bounce.UtilsTest.assertStatus;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.BounceApplication;
import com.bouncestorage.bounce.admin.BounceService;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.commons.lang.StringUtils;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class LRUStoragePolicyTest extends StoragePolicyTest {
    @Before
    public void setup() throws Exception {
        synchronized (BounceApplication.class) {
            app = new BounceApplication();
        }
        app.useRandomPorts();
        app.registerConfigurationListener();
        app.pauseBackgroundTasks();
        bounceService = new BounceService(app);

        UtilsTest.createTestProvidersConfig(app.getConfiguration());
        containerName = UtilsTest.switchPolicyforContainer(app, LRUStoragePolicy.class,
                ImmutableMap.of(StoragePolicy.CAPACITY_SETTING, Long.toString(10000L)));
        policy = (StoragePolicy) app.getBlobStore(containerName);

        // need to initialize logger after dropwizard application init
        logger = LoggerFactory.getLogger(WriteBackPolicyTest.class);
    }

    @Test
    public void testMoveObjectLRU() throws Exception {
        String expiringBlob = "expired";
        String recentBlob = "recent";
        String content = StringUtils.repeat("foo", 3000);
        Blob blob = UtilsTest.makeBlob(policy, expiringBlob, ByteSource.wrap(content.getBytes()));
        policy.putBlob(containerName, blob);

        blob = UtilsTest.makeBlob(policy, recentBlob, ByteSource.wrap(content.getBytes()));
        policy.putBlob(containerName, blob);

        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        policy.getBlob(containerName, recentBlob);

        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertStatus(status, status::getMovedObjectCount).isEqualTo(1);

        BlobMetadata link = policy.getSource().blobMetadata(containerName, expiringBlob);
        assertThat(BounceLink.isLink(link)).isTrue();

        link = policy.getSource().blobMetadata(containerName, recentBlob);
        assertThat(BounceLink.isLink(link)).isFalse();
    }

}
