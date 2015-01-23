/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.UtilsTest;
import com.google.common.collect.ImmutableMap;
import org.jclouds.Constants;
import org.jclouds.blobstore.BlobStoreContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public final class ConfigTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private BlobStoreContext bounceContext;
    private BounceBlobStore bounceBlobStore;
    private String containerName;
    private BounceApplication app;
    private ConfigurationResource backendConfig;

    @Before
    public void setUp() throws Exception {
        containerName = UtilsTest.createRandomContainerName();

        String config = getClass().getResource("/bounce.yml").toExternalForm();
        backendConfig = UtilsTest.createConfigurationResource();
        app = new BounceApplication(backendConfig);
        app.useRandomPorts();
        app.run(new String[]{
                "server", config
        });
        backendConfig.addBlobStoreListener(context -> {
            bounceBlobStore = (BounceBlobStore) context.getBlobStore();
            bounceContext = context;
            app.useBlobStore(bounceBlobStore);
        });
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
    public void testUpdateConfig() throws Exception {
        setTransientBackend();
        assertThat(app.getBlobStore()).isNotNull();
        ServiceStats stats = new ServiceResource(app).getServiceStats();
        assertThat(stats.getContainerNames()).isEmpty();
    }

    @Test
    public void testGetConfig() throws Exception {
        assertThat(backendConfig.getConfig()).isEmpty();
        setTransientBackend();
        assertThat(backendConfig.getConfig()).hasSize(2);
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

        backendConfig.updateConfig(properties);
    }
}
