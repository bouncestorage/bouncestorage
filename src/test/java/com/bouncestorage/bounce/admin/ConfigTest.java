/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

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

import java.util.HashMap;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

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

        configuration = new MapConfiguration(new HashMap<>());
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
        assertThat(config.getConfig()).isEmpty();
        setTransientBackend();
        assertThat(config.getConfig()).hasSize(2);
    }

    @Test
    public void testConfigBounceEverythingPolicy() throws Exception {
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
                "bounce.service.bounce-policy", "BounceEverythingPolicy"
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
