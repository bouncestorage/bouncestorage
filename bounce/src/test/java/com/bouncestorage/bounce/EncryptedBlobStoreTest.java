/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;

import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.io.ContentMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public final class EncryptedBlobStoreTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private BlobStoreContext context;
    private EncryptedBlobStore blobStore;
    private String containerName;

    @Before
    public void setUp() throws Exception {
        containerName = Utils.createRandomContainerName();
        Properties properties = new Properties();
        properties.putAll(ImmutableMap.of(
                Constants.PROPERTY_PROVIDER, "encrypted",
                EncryptedBlobStore.KEY, "foobar",
                EncryptedBlobStore.SALT, "salty"
        ));
        Utils.insertAllWithPrefix(properties,
                EncryptedBlobStore.BACKEND + ".",
                ImmutableMap.of(
                        Constants.PROPERTY_PROVIDER, "transient"
                ));
        context = ContextBuilder
                .newBuilder("encrypted")
                .overrides(properties)
                .build(BlobStoreContext.class);
        blobStore = (EncryptedBlobStore) context.getBlobStore();
        blobStore.createContainerInLocation(null, containerName);
    }

    @After
    public void tearDown() throws Exception {
        context.close();
    }

    @Test
    public void testEncrypted() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        ByteSource byteSource = ByteSource.wrap(new byte[new Random().nextInt(10)]);
        Blob blob = UtilsTest.makeBlob(blobStore, blobName, byteSource);
        blobStore.putBlob(containerName, blob);

        expectedException.expect(AssertionError.class);
        Blob blob2 = blobStore.delegate().getBlob(containerName, blobName);
        try (InputStream is = blob2.getPayload().openStream();
             InputStream is2 = byteSource.openStream()) {
            assertThat(is).hasContentEqualTo(is2);
        }

    }

    @Test
    public void testPut() throws Exception {
        String blobName = UtilsTest.createRandomBlobName();
        ByteSource byteSource = ByteSource.wrap(new byte[new Random().nextInt(10)]);
        Blob blob = UtilsTest.makeBlob(blobStore, blobName, byteSource);
        ContentMetadata metadata = blob.getMetadata().getContentMetadata();
        blobStore.putBlob(containerName, blob);

        Blob blob2 = blobStore.getBlob(containerName, blobName);
        try (InputStream is = blob2.getPayload().openStream();
             InputStream is2 = byteSource.openStream()) {
            assertThat(is).hasContentEqualTo(is2);
        }

        assertThat(blob2.getMetadata().getSize()).isEqualTo(blob.getMetadata().getSize());

        // TODO: assert more metadata, including user metadata
        ContentMetadata metadata2 = blob2.getMetadata().getContentMetadata();
        // don't check content MD5 because we transform the content
        assertThat(metadata2.getContentType()).isEqualTo(
                metadata.getContentType());
    }
}
