/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import com.bouncestorage.bounce.admin.BounceApplication;
import com.bouncestorage.bounce.admin.BounceConfiguration;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.bouncestorage.bounce.admin.Location;
import com.bouncestorage.bounce.admin.VirtualContainer;
import com.bouncestorage.bounce.admin.VirtualContainerResource;
import com.bouncestorage.bounce.admin.policy.WriteBackPolicy;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteSource;
import com.google.common.net.MediaType;
import com.google.inject.Module;

import org.apache.commons.configuration.Configuration;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.ListContainerOptions;
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

    public static void switchPolicyforContainer(BounceApplication app, String container, Class<? extends BouncePolicy>
            policy) {
        switchPolicyforContainer(app, container, policy, ImmutableMap.of());
    }

    public static void switchPolicyforContainer(BounceApplication app, String container,
            Class<? extends BouncePolicy> policy, Map<String, String> policyConfig) {
        BounceConfiguration config = app.getConfiguration();
        assertThat(config.getList("bounce.backends").size() >= 2).isTrue();
        String containerPrefix = Joiner.on(".").join(VirtualContainerResource.VIRTUAL_CONTAINER_PREFIX, "0");
        String cacheTierPrefix = Joiner.on(".").join(containerPrefix, VirtualContainer.CACHE_TIER_PREFIX);
        Properties newProperties = new Properties();
        newProperties.setProperty(Joiner.on(".").join(cacheTierPrefix, Location.BLOB_STORE_ID_FIELD), "0");
        newProperties.setProperty(Joiner.on(".").join(cacheTierPrefix, "policy"), policy.getSimpleName());
        if (policyConfig.get(WriteBackPolicy.COPY_DELAY) != null) {
            newProperties.setProperty(Joiner.on(".").join(cacheTierPrefix, WriteBackPolicy.COPY_DELAY),
                    policyConfig.get(WriteBackPolicy.COPY_DELAY));
        }
        if (policyConfig.get(WriteBackPolicy.EVICT_DELAY) != null) {
            newProperties.setProperty(Joiner.on(".").join(cacheTierPrefix, WriteBackPolicy.EVICT_DELAY),
                    policyConfig.get(WriteBackPolicy.EVICT_DELAY));
        }
        policyConfig.entrySet()
                .forEach(entry -> config.setProperty(Joiner.on(".").join(cacheTierPrefix, entry.getKey()),
                        entry.getValue()));
        String primaryTierPrefix = Joiner.on(".").join(containerPrefix, VirtualContainer.PRIMARY_TIER_PREFIX);
        newProperties.setProperty(Joiner.on(".").join(primaryTierPrefix, Location.BLOB_STORE_ID_FIELD), "1");
        newProperties.setProperty(Joiner.on(".").join(primaryTierPrefix, Location.CONTAINER_NAME_FIELD),
                container + "-dest");
        newProperties.setProperty(Joiner.on(".").join(containerPrefix, VirtualContainer.NAME), container);

        config.setProperty("bounce.containers", config.getList("bounce.containers").add(0));
        config.setAll(newProperties);
    }

    public static void createTestProvidersConfig(BounceConfiguration config) {
        Properties properties = new Properties();
        Properties systemProperties = System.getProperties();
        loadWithPrefix(properties, systemProperties, "bounce.backend.0");
        loadWithPrefix(properties, systemProperties, "bounce.backend.1");
        ImmutableSet.of("bounce.backend.0", "bounce.backend.1")
                .stream()
                .filter(key -> !properties.containsKey(key + "." + Constants.PROPERTY_PROVIDER))
                .forEach(key -> Utils.insertAllWithPrefix(properties, key + ".", ImmutableMap.of(
                        Constants.PROPERTY_PROVIDER, "transient")));
        properties.setProperty("bounce.backends", "0,1");
        config.setAll(properties);
    }

    public static void createTransientProviderConfig(Configuration config) {
        List<Object> backends = config.getList("bounce.backends");
        int lastId = backends.stream()
                .map(id -> Integer.valueOf(id.toString()))
                .reduce(Math::max)
                .orElse(-1);
        backends = new ArrayList<>(backends);
        lastId++;
        backends.add(lastId);
        config.setProperty("bounce.backend." + lastId + ".jclouds.provider", "transient");
        config.setProperty("bounce.backends", backends);
    }

    public static BlobStore createTransientBlobStore() {
        return ContextBuilder.newBuilder("transient").build(BlobStoreContext.class).getBlobStore();
    }

    public static void assertEqualBlobs(Blob one, Blob two) throws Exception {
        try (InputStream is = one.getPayload().openStream();
             InputStream is2 = two.getPayload().openStream()) {
            assertThat(is2).hasContentEqualTo(is);
        }
        // TODO: assert more metadata, including user metadata
        ContentMetadata metadata = one.getMetadata().getContentMetadata();
        ContentMetadata metadata2 = two.getMetadata().getContentMetadata();
        assertThat(metadata2.getContentMD5AsHashCode()).isEqualTo(
                metadata.getContentMD5AsHashCode());
        assertThat(metadata2.getContentType()).isEqualTo(
                metadata.getContentType());
    }

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
                .userMetadata(ImmutableMap.of("key1", "value1"))
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
            assertThat(blob2.getMetadata().getUserMetadata()).isEqualTo(
                    blob.getMetadata().getUserMetadata());
            assertThat(is2).hasContentEqualTo(is);
        }
        assertThat(nearBlobStore.blobExists(containerName, blobName))
                .isFalse();
    }

    @Test
    public void testCrawlWithPagination() throws Exception {
        for (int i = 0; i < 5; ++i) {
            String blobName = "blob" + i;
            ByteSource byteSource = ByteSource.wrap(new byte[1]);
            Blob blob = nearBlobStore.blobBuilder(blobName)
                    .payload(byteSource)
                    .contentLength(byteSource.size())
                    .contentMD5(byteSource.hash(Hashing.md5()))
                    .contentType(MediaType.OCTET_STREAM)
                    .build();
            nearBlobStore.putBlob(containerName, blob);
        }

        assertThat(Utils.crawlBlobStore(nearBlobStore, containerName,
                new ListContainerOptions().maxResults(2))).hasSize(5);
    }

    @Test
    public void testBounceDirectories() throws Exception {
        String blobName = "foo/bar/baz";
        ByteSource byteSource = ByteSource.wrap(new byte[1]);
        Blob blob = nearBlobStore.blobBuilder(blobName)
                .payload(byteSource)
                .contentLength(byteSource.size())
                .contentMD5(byteSource.hash(Hashing.md5()))
                .contentType(MediaType.OCTET_STREAM)
                .build();
        nearBlobStore.putBlob(containerName, blob);

        assertThat(Utils.crawlBlobStore(nearBlobStore, containerName))
                .hasSize(1);

        for (StorageMetadata sm : Utils.crawlBlobStore(nearBlobStore,
                containerName)) {
            Utils.moveBlob(nearBlobStore, farBlobStore, containerName,
                    containerName, sm.getName());
        }
    }

    public static String createRandomContainerName() {
        return "bounce-" + new Random().nextInt(Integer.MAX_VALUE);
    }

    public static String createRandomBlobName() {
        return "blob-" + new Random().nextInt(Integer.MAX_VALUE);
    }

    public static Blob makeBlob(BlobStore blobStore, String blobName,
                                ByteSource byteSource) throws IOException {
        return blobStore.blobBuilder(blobName)
                .payload(byteSource)
                .contentLength(byteSource.size())
                .contentType(MediaType.OCTET_STREAM)
                .contentMD5(byteSource.hash(Hashing.md5()))
                .build();
    }

    public static Blob makeBlob(BlobStore blobStore, String blobName) throws IOException {
        return makeBlob(blobStore, blobName, ByteSource.empty());
    }

    private static void loadWithPrefix(Properties out, Properties in, String prefix) {
        in.stringPropertyNames()
                .stream()
                .filter(key -> key.startsWith(prefix))
                .forEach(key -> out.put(key, in.getProperty(key)));
    }

    public static void advanceServiceClock(BounceApplication app, Duration duration) {
        app.setClock(Clock.offset(app.getClock(), duration));
    }
}
