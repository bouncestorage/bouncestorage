/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.UtilsTest;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.configuration.MapConfiguration;
import org.jclouds.Constants;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public final class ConfigTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private BlobStoreContext bounceContext;
    private BounceBlobStore bounceBlobStore;
    private String containerName;
    private BounceApplication app;
    private MapConfiguration configuration;

    @Before
    public void setUp() throws Exception {
        containerName = UtilsTest.createRandomContainerName();

        Properties properties = new Properties();
        try (InputStream is = ConfigTest.class.getResourceAsStream("/bounce.properties")) {
            properties.load(is);
        }
        configuration = new MapConfiguration((Map) properties);
        app = new BounceApplication(configuration);
        app.useRandomPorts();
    }

    @After
    public void tearDown() throws Exception {
        if (bounceBlobStore != null) {
            bounceBlobStore.deleteContainer(containerName);
        }
        if (bounceContext != null) {
            bounceContext.close();
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
        String[] blobStores = {BounceBlobStore.STORE_PROPERTY_1 + "." + Constants.PROPERTY_PROVIDER,
                BounceBlobStore.STORE_PROPERTY_2 + "." + Constants.PROPERTY_PROVIDER };
        for (String bs : blobStores) {
            assertThat(properties).doesNotContainKey(bs);
        }
        setTransientBackend();
        assertThat(properties).containsKeys(blobStores);
    }

    @Test
    public void testConfigMoveEverythingPolicy() throws Exception {
        setTransientBackend();
        BlobStore blobStore = app.getBlobStore();
        blobStore.createContainerInLocation(null, containerName);
        BounceService bounceService = app.getBounceService();
        blobStore.putBlob(containerName,
                UtilsTest.makeBlob(blobStore, UtilsTest.createRandomBlobName()));
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getTotalObjectCount()).isEqualTo(1);
        assertThat(status.getBouncedObjectCount()).isEqualTo(0);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);

        Properties properties = new Properties();
        properties.putAll(ImmutableMap.of(
                "bounce.service.bounce-policy", "MoveEverythingPolicy"
        ));
        new ConfigurationResource(app).updateConfig(properties);
        status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getTotalObjectCount()).isEqualTo(1);
        assertThat(status.getBouncedObjectCount()).isEqualTo(1);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);
    }

    @Test
    public void testConfigBounceLastModifiedPolicy() throws Exception {
        setTransientBackend();
        BlobStore blobStore = app.getBlobStore();
        blobStore.createContainerInLocation(null, containerName);
        BounceService bounceService = app.getBounceService();
        blobStore.putBlob(containerName,
                UtilsTest.makeBlob(blobStore, UtilsTest.createRandomBlobName()));
        BounceService.BounceTaskStatus status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getTotalObjectCount()).isEqualTo(1);
        assertThat(status.getBouncedObjectCount()).isEqualTo(0);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);

        Properties properties = new Properties();
        properties.putAll(ImmutableMap.of(
                "bounce.service.bounce-policy", "LastModifiedTimePolicy",
                "bounce.service.bounce-policy.duration", "PT1H"
        ));
        new ConfigurationResource(app).updateConfig(properties);
        status = bounceService.bounce(containerName);
        status.future().get();
        assertThat(status.getTotalObjectCount()).isEqualTo(1);
        assertThat(status.getBouncedObjectCount()).isEqualTo(0);
        assertThat(status.getErrorObjectCount()).isEqualTo(0);
    }

    private void setTransientBackend() throws Exception {
        Properties properties = new Properties();
        Utils.insertAllWithPrefix(properties,
                BounceBlobStore.STORE_PROPERTY_1 + ".",
                ImmutableMap.of(
                        Constants.PROPERTY_PROVIDER, "transient"
                ));
        Utils.insertAllWithPrefix(properties,
                BounceBlobStore.STORE_PROPERTY_2 + ".",
                ImmutableMap.of(
                        Constants.PROPERTY_PROVIDER, "transient"
                ));

        new ConfigurationResource(app).updateConfig(properties);
    }
}
