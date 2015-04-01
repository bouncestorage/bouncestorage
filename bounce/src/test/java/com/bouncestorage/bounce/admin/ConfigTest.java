/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;

import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.policy.LastModifiedTimePolicy;
import com.bouncestorage.bounce.admin.policy.MoveEverythingPolicy;
import com.google.common.collect.ImmutableMap;

import org.jclouds.blobstore.BlobStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public final class ConfigTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private String containerName;
    private BounceApplication app;
    private BounceService bounceService;

    @Before
    public void setUp() throws Exception {
        containerName = UtilsTest.createRandomContainerName();

        Properties properties = new Properties();
        try (InputStream is = ConfigTest.class.getResourceAsStream("/bounce.properties")) {
            properties.load(is);
        }

        synchronized (BounceApplication.class) {
            app = new BounceApplication();
        }
        app.getConfiguration().setAll(properties);
        app.useRandomPorts();
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
        expectedException.expect(NullPointerException.class);
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
        assertThat(properties).doesNotContainKeys(blobStores);
        setTransientBackend();
        assertThat(properties).containsKeys(blobStores);
    }

    @Test
    public void testConfigMoveEverythingPolicy() throws Exception {
        setTransientBackend();

        BlobStore blobStore = app.getBlobStore(containerName);
        blobStore.createContainerInLocation(null, containerName);

        blobStore.putBlob(containerName,
                UtilsTest.makeBlob(blobStore, UtilsTest.createRandomBlobName()));

        Properties properties = new Properties();
        properties.putAll(ImmutableMap.of(
                "bounce.container.0.tier.0.backend", "0",
                "bounce.container.0.tier.0.policy", MoveEverythingPolicy.class.getSimpleName(),
                "bounce.container.0.tier.1.backend", "1",
                "bounce.container.0.name", containerName,
                "bounce.containers", "0"
        ));
        new ConfigurationResource(app).updateConfig(properties);
        blobStore = app.getBlobStore(containerName);
        blobStore.createContainerInLocation(null, containerName);

        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getTotalObjectCount()).isEqualTo(1);
        assertThat(status.getMovedObjectCount()).isEqualTo(1);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);
    }

    @Test
    public void testConfigBounceLastModifiedPolicy() throws Exception {
        setTransientBackend();
        BlobStore blobStore = app.getBlobStore();
        blobStore.createContainerInLocation(null, containerName);

        blobStore.putBlob(containerName,
                UtilsTest.makeBlob(blobStore, UtilsTest.createRandomBlobName()));

        Properties properties = new Properties();
        properties.putAll(ImmutableMap.of(
                "bounce.container.0.tier.0.backend", "0",
                "bounce.container.0.tier.0.policy", LastModifiedTimePolicy.class.getSimpleName(),
                "bounce.container.0.tier.0.evict-delay", Duration.ofHours(1).toString(),
                "bounce.container.0.tier.1.backend", "1",
                "bounce.container.0.name", containerName
        ));
        properties.setProperty("bounce.containers", "0");
        new ConfigurationResource(app).updateConfig(properties);
        blobStore = app.getBlobStore(containerName);
        blobStore.createContainerInLocation(null, containerName);

        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getTotalObjectCount()).isEqualTo(1);
        assertThat(status.getMovedObjectCount()).isEqualTo(0);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);
    }

    private void setTransientBackend() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bounce.backend.0.jclouds.provider", "transient");
        properties.setProperty("bounce.backend.1.jclouds.provider", "transient");
        properties.setProperty("bounce.backends", "0,1");

        new ConfigurationResource(app).updateConfig(properties);
    }
}
