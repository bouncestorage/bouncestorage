/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static com.bouncestorage.bounce.UtilsTest.assertStatus;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import com.bouncestorage.bounce.BlobStoreTarget;
import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.policy.WriteBackPolicy;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class ConfigTest {
    private String containerName;
    private BounceApplication app;
    private BounceService bounceService;

    @Before
    public void setUp() throws Exception {
        containerName = Utils.createRandomContainerName();

        Properties properties = new Properties();
        try (InputStream is = ConfigTest.class.getResourceAsStream("/bounce.properties")) {
            properties.load(is);
        }

        synchronized (BounceApplication.class) {
            app = new BounceApplication();
        }
        app.getConfiguration().setAll(properties);
        app.useRandomPorts();
        app.registerConfigurationListener();
        bounceService = new BounceService(app);
    }

    @After
    public void tearDown() throws Exception {
        BlobStore blobStore = app.getBlobStore();
        if (blobStore != null) {
            blobStore.deleteContainer(containerName);
        }
        if (blobStore != null && blobStore.getContext() != null) {
            blobStore.getContext().close();
        }

        app.stop();
    }

    @Test
    public void testWithoutConfig() throws Exception {
        new ServiceResource(app).getServiceStats();
    }

    @Test
    public void testConfigBackend() throws Exception {
        setTransientBackend();
        assertThat(app.getBlobStore()).isNotNull();
        ServiceStats stats = new ServiceResource(app).getServiceStats();
        assertThat(stats.getContainerNames()).isEmpty();
    }

    @Test
    public void testGetConfig() throws Exception {
        ConfigurationResource config = new ConfigurationResource(app);
        Properties properties = config.getConfig();
        String[] blobStores = {
                "bounce.backend.0.jclouds.provider",
                "bounce.backend.1.jclouds.provider"
        };
        assertThat(properties).doesNotContainKeys((Object[]) blobStores);
        setTransientBackend();
        assertThat(properties).containsKeys((Object[]) blobStores);
    }

    @Test
    public void testConfigMoveEverythingPolicy() throws Exception {
        setTransientBackend();
        app.getBlobStore(0).createContainerInLocation(null, containerName);
        app.getBlobStore(1).createContainerInLocation(null, containerName);

        BlobStore blobStore = app.getBlobStore(0);
        blobStore.createContainerInLocation(null, containerName);

        blobStore.putBlob(containerName,
                UtilsTest.makeBlob(blobStore, UtilsTest.createRandomBlobName()));

        configureMoveEverythingPolicy();
        blobStore = app.getBlobStore(containerName);
        blobStore.createContainerInLocation(null, containerName);

        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertStatus(status, status::getTotalObjectCount).isEqualTo(1);
        assertStatus(status, status::getMovedObjectCount).isEqualTo(1);
        assertStatus(status, status::getErrorObjectCount).isEqualTo(0);
    }

    private void configureMoveEverythingPolicy() {
        Properties properties = new Properties();
        String prefix = VirtualContainerResource.VIRTUAL_CONTAINER_PREFIX + ".0";
        String tier0prefix = prefix + "." + VirtualContainer.CACHE_TIER_PREFIX;
        String tier1prefix = prefix + "." + VirtualContainer.PRIMARY_TIER_PREFIX;
        Map<Object, Object> m = ImmutableMap.builder()
                .put(prefix + ".name", containerName)
                .put(tier0prefix + ".backend", "0")
                .put(tier0prefix + ".policy", WriteBackPolicy.class.getSimpleName())
                .put(tier0prefix + ".copyDelay", Duration.ofHours(-1).toString())
                .put(tier0prefix + ".evictDelay", Duration.ofHours(0).toString())
                .put(tier1prefix + ".backend", "1")
                .put("bounce.containers", "0")
                .build();
        properties.putAll(m);
        new ConfigurationResource(app).updateConfig(properties);
    }

    @Test
    public void testConfigBounceLastModifiedPolicy() throws Exception {
        setTransientBackend();
        app.getBlobStore(0).createContainerInLocation(null, containerName);
        app.getBlobStore(1).createContainerInLocation(null, containerName);
        BlobStore blobStore = app.getBlobStore();

        blobStore.putBlob(containerName,
                UtilsTest.makeBlob(blobStore, UtilsTest.createRandomBlobName()));

        Properties properties = new Properties();
        String containerPrefix = Joiner.on(".").join(VirtualContainerResource.VIRTUAL_CONTAINER_PREFIX,
                "0");
        String cachePrefix = Joiner.on(".").join(containerPrefix, VirtualContainer.CACHE_TIER_PREFIX);
        String primaryPrefix = Joiner.on(".").join(containerPrefix, VirtualContainer.PRIMARY_TIER_PREFIX);
        properties.putAll(ImmutableMap.of(
                Joiner.on(".").join(cachePrefix, Location.BLOB_STORE_ID_FIELD), "0",
                Joiner.on(".").join(cachePrefix, "policy"), WriteBackPolicy.class.getSimpleName(),
                Joiner.on(".").join(cachePrefix, WriteBackPolicy.EVICT_DELAY), Duration.ofHours(1).toString(),
                Joiner.on(".").join(cachePrefix, WriteBackPolicy.COPY_DELAY), Duration.ofHours(-1).toString()
        ));
        properties.putAll(ImmutableMap.of(
                Joiner.on(".").join(primaryPrefix, Location.BLOB_STORE_ID_FIELD), "1",
                Joiner.on(".").join(containerPrefix, VirtualContainer.NAME), containerName
        ));
        properties.setProperty("bounce.containers", "0");
        new ConfigurationResource(app).updateConfig(properties);
        blobStore = app.getBlobStore(containerName);

        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertStatus(status, status::getTotalObjectCount).isEqualTo(1);
        assertStatus(status, status::getMovedObjectCount).isEqualTo(0);
        assertStatus(status, status::getErrorObjectCount).isEqualTo(0);
    }

    @Test
    public void testConfigureThreeTiers() throws Exception {
        Properties p = new Properties();
        p.put("bounce.backend.0.jclouds.provider", "transient");
        p.put("bounce.backend.1.jclouds.provider", "transient");
        p.put("bounce.backend.2.jclouds.provider", "transient");
        p.put("bounce.backends", "0,1,2");

        new ConfigurationResource(app).updateConfig(p);

        app.getBlobStore(0).createContainerInLocation(null, containerName);
        app.getBlobStore(1).createContainerInLocation(null, containerName);
        app.getBlobStore(2).createContainerInLocation(null, containerName);

        Map<Object, Object> m = ImmutableMap.builder()
                .put("bounce.container.0.name", containerName)
                .put("bounce.container.0.identity", "identity")
                .put("bounce.container.0.credential", "credential")
                .put("bounce.container.0.tier.0.backend", "0")
                .put("bounce.container.0.tier.0.policy", WriteBackPolicy.class.getSimpleName())
                .put("bounce.container.0.tier.0.copyDelay", Duration.ofHours(0).toString())
                .put("bounce.container.0.tier.0.evictDelay", Duration.ofHours(1).toString())
                .put("bounce.container.0.tier.1.backend", "1")
                .put("bounce.container.0.tier.1.policy", WriteBackPolicy.class.getSimpleName())
                .put("bounce.container.0.tier.1.copyDelay", Duration.ofHours(0).toString())
                .put("bounce.container.0.tier.1.evictDelay", Duration.ofHours(1).toString())
                .put("bounce.container.0.tier.2.backend", "2")
                .put("bounce.container.0.tier.2.policy", WriteBackPolicy.class.getSimpleName())
                .put("bounce.container.0.tier.2.copyDelay", Duration.ofHours(0).toString())
                .put("bounce.container.0.tier.2.evictDelay", Duration.ofHours(1).toString())
                .put("bounce.containers", "0")
                .build();
        p = new Properties();
        p.putAll(m);
        new ConfigurationResource(app).updateConfig(p);
        Map.Entry<String, BlobStore> entry = app.locateBlobStore("identity", containerName, null);
        assertThat(entry).isNotNull();
        assertThat(entry.getKey()).isEqualTo("credential");
        assertThat(entry.getValue()).isInstanceOf(BouncePolicy.class);
        BouncePolicy policy = (BouncePolicy) entry.getValue();
        assertThat(policy.getSource()).isInstanceOf(BlobStoreTarget.class);
        assertThat(policy.getDestination()).isInstanceOf(BouncePolicy.class);
        policy = (BouncePolicy) policy.getDestination();
        assertThat(policy.getSource()).isInstanceOf(BlobStoreTarget.class);
        assertThat(policy.getDestination()).isInstanceOf(BlobStoreTarget.class);
    }

    @Test
    public void testConfigureTakeOver() throws Exception {
        setTransientBackend();
        app.getBlobStore(0).createContainerInLocation(null, containerName);
        app.getBlobStore(1).createContainerInLocation(null, containerName);
        String blobName = UtilsTest.createRandomBlobName();
        Blob blob = UtilsTest.makeBlob(app.getBlobStore(1), blobName);
        app.getBlobStore(1).putBlob(containerName, blob);
        configureMoveEverythingPolicy();
        assertThat(app.getBlobStore(containerName)).isInstanceOf(BouncePolicy.class);
        BouncePolicy policy = (BouncePolicy) app.getBlobStore(containerName);
        assertThat(policy.blobExists(containerName, blobName));
        policy.waitForTakeOver();
        assertThat(policy.blobExists(containerName, blobName)).isTrue();
        assertThat(BounceLink.isLink(app.getBlobStore(0).blobMetadata(containerName, blobName))).isTrue();
    }

    private void setTransientBackend() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bounce.backend.0.jclouds.provider", "transient");
        properties.setProperty("bounce.backend.1.jclouds.provider", "transient");
        properties.setProperty("bounce.backends", "0,1");

        new ConfigurationResource(app).updateConfig(properties);
    }
}
