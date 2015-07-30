/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static com.bouncestorage.bounce.UtilsTest.assertStatus;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.TreeMap;

import com.bouncestorage.bounce.BlobStoreTarget;
import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.BounceApplication;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.bouncestorage.bounce.admin.BounceService;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteSource;

import org.apache.commons.lang.StringUtils;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.blobstore.domain.internal.PageSetImpl;
import org.jclouds.blobstore.domain.internal.StorageMetadataImpl;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.exceptions.base.MockitoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoragePolicyTest {
    protected BounceApplication app;
    protected BounceService bounceService;
    protected String containerName;
    protected Logger logger;
    protected StoragePolicy policy;

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
        containerName = UtilsTest.switchPolicyforContainer(app, StoragePolicy.class,
                ImmutableMap.of(StoragePolicy.CAPACITY_SETTING, Long.toString(10000L)));
        policy = (StoragePolicy) app.getBlobStore(containerName);

        // need to initialize logger after dropwizard application init
        logger = LoggerFactory.getLogger(WriteBackPolicyTest.class);
    }

    @After
    public void tearDown() throws Exception {
        if (policy != null && containerName != null) {
            policy.deleteContainer(containerName);
        }
    }

    @Test
    public void testEvictedDaysCalculation() {
        TreeMap<Instant, Long> sizeHistogram = new TreeMap<>();
        policy.currentSize = 12000L;

        Instant expired = Instant.now().truncatedTo(ChronoUnit.DAYS);
        sizeHistogram.put(expired, 4000L);

        Instant later = Instant.now().truncatedTo(ChronoUnit.DAYS).plus(1, ChronoUnit.DAYS);
        sizeHistogram.put(later, 8000L);

        Instant evictionTime = policy.getEvictionTime(sizeHistogram);
        assertThat(evictionTime).isEqualTo(expired);
        assertThat(evictionTime.isBefore(expired.plus(1, ChronoUnit.SECONDS))).isTrue();
    }

    @Test
    public void testEvictedDaysEdgeCase() {
        TreeMap<Instant, Long> sizeHistogram = new TreeMap<>();
        policy.currentSize = 18000L;

        Instant expiredDay = Instant.now();
        sizeHistogram.put(expiredDay, 4000L);
        sizeHistogram.put(Instant.now().plus(1, ChronoUnit.DAYS), 5000L);

        sizeHistogram.put(Instant.now().plus(2, ChronoUnit.DAYS), 9000L);

        Instant evictionTime = policy.getEvictionTime(sizeHistogram);
        assertThat(evictionTime.isAfter(expiredDay.plus(1, ChronoUnit.DAYS)));
        assertThat(evictionTime.isBefore(expiredDay.plus(1, ChronoUnit.DAYS).plus(1, ChronoUnit.SECONDS))).isTrue();
    }

    @Test
    public void testMoveObject() throws Exception {
        String expiringBlob = "expired";
        String content = StringUtils.repeat("foo", 3500);
        Blob blob = UtilsTest.makeBlob(policy, expiringBlob, ByteSource.wrap(content.getBytes()));
        policy.putBlob(containerName, blob);
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertStatus(status, status::getErrorObjectCount).isEqualTo(0);
        assertStatus(status, status::getMovedObjectCount).isEqualTo(1);

        String farContainer = ((BlobStoreTarget) policy.getDestination()).mapContainer(null);
        Blob farBlob = policy.getDestination().getBlob(farContainer, expiringBlob);
        assertThat(farBlob.getMetadata().getContentMetadata().getContentLength()).isEqualTo(content.length());

        BlobMetadata link = policy.getSource().blobMetadata(containerName, expiringBlob);
        assertThat(BounceLink.isLink(link)).isTrue();
    }

    @Test
    public void testDirectoryBlobMoveNOOP() throws Exception {
        String expiringBlob = "expired";
        String content = StringUtils.repeat("foo", 3500);
        Blob blob = UtilsTest.makeBlob(policy, expiringBlob, ByteSource.wrap(content.getBytes()));
        policy.putBlob(containerName, blob);

        ByteSource dirBlobPayload = ByteSource.empty();
        String directoryBlob = "dir";
        Blob dirBlob = policy.blobBuilder(directoryBlob)
                .payload(dirBlobPayload)
                .contentLength(dirBlobPayload.size())
                .contentType("application/directory")
                .contentMD5(dirBlobPayload.hash(Hashing.md5()))
                .build();
        policy.putBlob(containerName, dirBlob);

        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertStatus(status, status::getErrorObjectCount).isEqualTo(0);
        assertStatus(status, status::getMovedObjectCount).isEqualTo(1);

        String farContainer = ((BlobStoreTarget) policy.getDestination()).mapContainer(null);
        Blob farBlob = policy.getDestination().getBlob(farContainer, expiringBlob);
        assertThat(farBlob.getMetadata().getContentMetadata().getContentLength()).isEqualTo(content.length());

        BlobMetadata link = policy.getSource().blobMetadata(containerName, expiringBlob);
        assertThat(BounceLink.isLink(link)).isTrue();

        BlobMetadata dirBlobMeta = policy.getSource().blobMetadata(containerName, directoryBlob);
        assertThat(BounceLink.isLink(dirBlobMeta)).isFalse();
    }

    @Test
    public void testMoveSomeObjects() throws Exception {
        String expiredBlob = "blob";
        Date expiredTime = Date.from(Instant.now());
        long expiredSize = 8000L;

        String remainingBlob = "staying";
        Date remainingTime = Date.from(Instant.now().plus(1, ChronoUnit.DAYS));
        long remainingSize = 4000L;

        BounceStorageMetadata expiredBlobMetadata = new BounceStorageMetadata(
                new StorageMetadataImpl(StorageType.BLOB, null, expiredBlob, null, null, null, expiredTime,
                        expiredTime, ImmutableMap.of(), expiredSize),
                BounceStorageMetadata.NEAR_ONLY);
        BounceStorageMetadata remainingBlobMetadata = new BounceStorageMetadata(
                new StorageMetadataImpl(StorageType.BLOB, null, remainingBlob, null, null, null, remainingTime,
                        remainingTime, ImmutableMap.of(), remainingSize),
                BounceStorageMetadata.NEAR_ONLY
        );
        ImmutableSet<StorageMetadata> listResults = ImmutableSet.of(expiredBlobMetadata, remainingBlobMetadata);

        StoragePolicy mock = Mockito.spy(policy);
        BlobStore mockSource = Mockito.mock(BlobStore.class);
        Mockito.doReturn(policy.getSource().getContext()).when(mock).getContext();
        Mockito.doReturn(mockSource).when(mock).getSource();
        Mockito.doReturn(new PageSetImpl<>(listResults, null)).when(mockSource).list(Mockito.anyString(),
                Mockito.any(ListContainerOptions.class));
        mock.prepareBounce(containerName);
        try {
            BouncePolicy.BounceResult result = mock.reconcileObject(containerName,
                    new BounceStorageMetadata(expiredBlobMetadata, BounceStorageMetadata.NEAR_ONLY), null);
            assertThat(result).isEqualTo(BouncePolicy.BounceResult.MOVE);
            result = mock.reconcileObject(containerName,
                    new BounceStorageMetadata(remainingBlobMetadata, BounceStorageMetadata.NEAR_ONLY), null);
            assertThat(result).isEqualTo(BouncePolicy.BounceResult.NO_OP);
        } catch (MockitoException e) {
            e.setStackTrace(e.getUnfilteredStackTrace());
            throw e;
        }
    }

    @Test
    public void testNoObjectsMoved() throws Exception {
        String content = StringUtils.repeat("foo", 2000);
        Blob blob = UtilsTest.makeBlob(policy, "blob", ByteSource.wrap(content.getBytes()));
        policy.putBlob(containerName, blob);
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertStatus(status, status::getErrorObjectCount).isEqualTo(0);
        assertStatus(status, status::getMovedObjectCount).isEqualTo(0);
    }
}
