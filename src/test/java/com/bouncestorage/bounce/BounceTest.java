/*
 * Copyright 2015 Bounce Storage <info@bouncestorage.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bouncestorage.bounce;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteSource;
import com.google.common.net.MediaType;
import com.google.inject.Module;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.io.ContentMetadata;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class BounceTest {
    private BlobStoreContext nearContext;
    private BlobStoreContext farContext;
    private BlobStore nearBlobStore;
    private BlobStore farBlobStore;
    private BounceBlobStore bounceBlobStore;
    private String containerName;

    @Before
    public void setUp() throws Exception {
        containerName = UtilsTest.createRandomContainerName();

        nearContext = ContextBuilder
                .newBuilder("transient")
                .credentials("identity", "credential")
                .modules(ImmutableList.<Module>of(new SLF4JLoggingModule()))
                .build(BlobStoreContext.class);
        nearBlobStore = nearContext.getBlobStore();

        farContext = ContextBuilder
                .newBuilder("transient")
                .credentials("identity", "credential")
                .modules(ImmutableList.<Module>of(new SLF4JLoggingModule()))
                .build(BlobStoreContext.class);
        farBlobStore = farContext.getBlobStore();

        bounceBlobStore = new BounceBlobStore();
        bounceBlobStore.setNearStore(nearBlobStore);
        bounceBlobStore.setFarStore(farBlobStore);
        bounceBlobStore.createContainerInLocation(null, containerName);
    }

    @After
    public void tearDown() throws Exception {
        if (bounceBlobStore != null) {
            bounceBlobStore.deleteContainer(containerName);
            // TODO: close context?
        }
        if (nearContext != null) {
            nearContext.close();
        }
        if (farContext != null) {
            farContext.close();
        }
    }

    @Test
    public void testCreateLink() throws Exception {
        String blobName = "blob";
        ByteSource byteSource = ByteSource.wrap(new byte[1]);
        Blob blob = nearBlobStore.blobBuilder(blobName)
                .payload(byteSource)
                .contentLength(byteSource.size())
                .contentMD5(byteSource.hash(Hashing.md5()))
                .build();
        nearBlobStore.putBlob(containerName, blob);

        assertThat(BounceLink.isLink(nearBlobStore.blobMetadata(
                containerName, blobName))).isEqualTo(false);

        bounceBlobStore.copyBlobAndCreateBounceLink(containerName, blobName);

        assertThat(BounceLink.isLink(nearBlobStore.blobMetadata(
                containerName, blobName))).isEqualTo(true);
    }

    @Test
    public void testBounceBlob() throws Exception {
        String blobName = "blob";
        ByteSource byteSource = ByteSource.wrap(new byte[1]);
        Blob blob = bounceBlobStore.blobBuilder(blobName)
                .payload(byteSource)
                .contentLength(byteSource.size())
                .contentType(MediaType.OCTET_STREAM)
                .contentMD5(byteSource.hash(Hashing.md5()))
                .build();
        ContentMetadata metadata = blob.getMetadata().getContentMetadata();
        bounceBlobStore.putBlob(containerName, blob);

        bounceBlobStore.copyBlobAndCreateBounceLink(containerName, blobName);

        Blob blob2 = bounceBlobStore.getBlob(containerName, blobName);
        try (InputStream is = blob2.getPayload().openStream();
             InputStream is2 = byteSource.openStream()) {
            assertThat(is2).hasContentEqualTo(is);
        }
        // TODO: assert more metadata, including user metadata
        ContentMetadata metadata2 = blob2.getMetadata().getContentMetadata();
        assertThat(metadata2.getContentMD5AsHashCode()).isEqualTo(
                metadata.getContentMD5AsHashCode());
        assertThat(metadata2.getContentType()).isEqualTo(
                metadata.getContentType());
    }
}
