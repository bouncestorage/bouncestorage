/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.util.Random;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteSource;
import com.google.common.net.MediaType;
import com.google.inject.Module;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.io.ContentMetadata;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class UtilsTest {
    private BlobStoreContext nearContext;
    private BlobStoreContext farContext;
    private BlobStore nearBlobStore;
    private BlobStore farBlobStore;
    private String containerName;

    @Before
    public void setUp() throws Exception {
        containerName = createRandomContainerName();

        nearContext = ContextBuilder
                .newBuilder("transient")
                .credentials("identity", "credential")
                .modules(ImmutableList.<Module>of(new SLF4JLoggingModule()))
                .build(BlobStoreContext.class);
        nearBlobStore = nearContext.getBlobStore();
        nearBlobStore.createContainerInLocation(null, containerName);

        farContext = ContextBuilder
                .newBuilder("transient")
                .credentials("identity", "credential")
                .modules(ImmutableList.<Module>of(new SLF4JLoggingModule()))
                .build(BlobStoreContext.class);
        farBlobStore = farContext.getBlobStore();
        farBlobStore.createContainerInLocation(null, containerName);
    }

    @After
    public void tearDown() throws Exception {
        if (nearContext != null) {
            nearBlobStore.deleteContainer(containerName);
            nearContext.close();
        }
        if (farContext != null) {
            farBlobStore.deleteContainer(containerName);
            farContext.close();
        }
    }

    @Test
    public void testBounceBlob() throws Exception {
        String blobName = "blob";
        ByteSource byteSource = ByteSource.wrap(new byte[1]);
        Blob blob = nearBlobStore.blobBuilder(blobName)
                .payload(byteSource)
                .contentLength(byteSource.size())
                .contentMD5(byteSource.hash(Hashing.md5()))
                .contentType(MediaType.OCTET_STREAM)
                .build();
        ContentMetadata metadata = blob.getMetadata().getContentMetadata();
        nearBlobStore.putBlob(containerName, blob);

        for (StorageMetadata sm : Utils.crawlBlobStore(nearBlobStore,
                containerName)) {
            Utils.moveBlob(nearBlobStore, farBlobStore, containerName,
                    containerName, sm.getName());
        }

        Blob blob2 = farBlobStore.getBlob(containerName, blobName);
        ContentMetadata metadata2 = blob2.getMetadata().getContentMetadata();
        try (InputStream is = byteSource.openStream();
             InputStream is2 = blob2.getPayload().openStream()) {
            assertThat(metadata2.getContentDisposition()).isEqualTo(
                    metadata.getContentDisposition());
            assertThat(metadata2.getContentEncoding()).isEqualTo(
                    metadata.getContentEncoding());
            assertThat(metadata2.getContentLanguage()).isEqualTo(
                    metadata.getContentLanguage());
            assertThat(metadata2.getContentLength()).isEqualTo(
                    metadata.getContentLength());
            assertThat(metadata2.getContentMD5AsHashCode()).isEqualTo(
                    metadata.getContentMD5AsHashCode());
            assertThat(metadata2.getContentType()).isEqualTo(
                    metadata.getContentType());
            assertThat(metadata2.getExpires()).isEqualTo(
                    metadata.getExpires());
            assertThat(is2).hasContentEqualTo(is);
        }
        assertThat(nearBlobStore.blobExists(containerName, blobName))
                .isEqualTo(false);
    }

    static String createRandomContainerName() {
        return "bounce-" + new Random().nextInt(Integer.MAX_VALUE);
    }

    static String createRandomBlobName() {
        return "blob-" + new Random().nextInt(Integer.MAX_VALUE);
    }
}
