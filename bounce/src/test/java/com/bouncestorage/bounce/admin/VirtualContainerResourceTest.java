/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.admin.policy.WriteBackPolicy;
import com.google.common.base.Joiner;

import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class VirtualContainerResourceTest {
    private BounceApplication app;

    @Before
    public void setUp() throws Exception {
        app = new BounceApplication();
        app.useRandomPorts();
        app.getConfiguration().setAll(getDefaultProperties());
        String webConfig = VirtualContainerResourceTest.class.getResource("/bounce.yml").toExternalForm();
        synchronized (app.getClass()) {
            app.run(new String[]{"server", webConfig});
        }
    }

    @After
    public void tearDown() throws Exception {
        app.stop();
    }

    @Test
    public void testVirtualContainerPut() throws Exception {
        String jsonInput = "{\"cacheLocation\":{\"blobStoreId\":0,\"containerName\":\"cache\"," +
                "\"copyDelay\":\"P0D\",\"moveDelay\":\"P0D\"},\"originLocation\":{\"blobStoreId\":0," +
                "\"containerName\":\"container\",\"copyDelay\":\"P90D\",\"moveDelay\":\"P180D\"}," +
                "\"archiveLocation\":{\"blobStoreId\":0,\"containerName\":\"archive\",\"copyDelay\":null," +
                "\"moveDelay\":null},\"migrationTargetLocation\":{\"blobStoreId\":-1,\"containerName\":\"\"," +
                "\"copyDelay\":null,\"moveDelay\":null},\"name\":\"magic\"}";

        String url = String.format("http://localhost:%d/api/virtual_container/0", app.getPort());

        String response = submitRequest(url, HttpMethod.PUT, jsonInput);
        assertThat(response).isEqualToIgnoringCase("OK");
        String containerPrefix = Joiner.on(".").join(VirtualContainerResource.VIRTUAL_CONTAINER_PREFIX, "0");
        String primaryTierPrefix = Joiner.on(".").join(containerPrefix, VirtualContainer.PRIMARY_TIER_PREFIX);
        Configuration config = app.getConfiguration();
        assertThat(config.getProperty(Joiner.on(".").join(primaryTierPrefix, WriteBackPolicy
                .EVICT_DELAY))).isEqualTo("P180D");
        String archivePrefix = Joiner.on(".").join(containerPrefix, VirtualContainer.ARCHIVE_TIER_PREFIX);
        assertThat(config.getProperty(Joiner.on(".").join(archivePrefix, Location
                .BLOB_STORE_ID_FIELD))).isEqualTo("0");
        assertThat(config.getProperty(Joiner.on(".").join(archivePrefix, Location.CONTAINER_NAME_FIELD)))
                .isEqualTo("archive");
    }

    @Test
    public void testEnablingMigration() throws Exception {
        String jsonInput = "{\"cacheLocation\":{\"blobStoreId\":0,\"containerName\":\"other\"," +
                "\"copyDelay\":\"P0D\",\"moveDelay\":\"P0D\"},\"originLocation\":{\"blobStoreId\":0," +
                "\"containerName\":\"container\",\"copyDelay\":null,\"moveDelay\":null}," +
                "\"archiveLocation\":{\"blobStoreId\":-1,\"containerName\":\"\",\"copyDelay\":null," +
                "\"moveDelay\":null},\"migrationTargetLocation\":{\"blobStoreId\":0,\"containerName\":\"target\"," +
                "\"copyDelay\":null,\"moveDelay\":null},\"name\":\"magic\"}";

        String url = String.format("http://localhost:%d/api/virtual_container/0", app.getPort());
        String response = submitRequest(url, HttpMethod.PUT, jsonInput);
        assertThat(response).isEqualToIgnoringCase("OK");
        String containerPrefix = Joiner.on(".").join(VirtualContainerResource.VIRTUAL_CONTAINER_PREFIX, "0");
        String targetLocationPrefix = Joiner.on(".").join(containerPrefix, VirtualContainer.MIGRATION_TIER_PREFIX);
        Configuration config = app.getConfiguration();
        assertThat(config.getProperty(Joiner.on(".").join(targetLocationPrefix, Location.BLOB_STORE_ID_FIELD)))
                .isEqualTo("0");
        assertThat(config.getProperty(Joiner.on(".").join(targetLocationPrefix, Location.CONTAINER_NAME_FIELD)))
                .isEqualTo("target");
    }

    private String submitRequest(String url, String method, String json) throws Exception {
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestMethod(method);
        connection.setRequestProperty(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
        connection.setDoOutput(true);
        try (OutputStream os = connection.getOutputStream()) {
            os.write(json.getBytes(StandardCharsets.UTF_8));
        }

        return connection.getResponseMessage();
    }

    private Properties getDefaultProperties() {
        Properties properties = new Properties();
        String virtualContainerPrefix = VirtualContainerResource.VIRTUAL_CONTAINER_PREFIX + "." + 0;
        properties.setProperty(Joiner.on(".").join(virtualContainerPrefix, VirtualContainer.NAME), "vBucket");
        String primaryTierPrefix = Joiner.on(".").join(virtualContainerPrefix, VirtualContainer.PRIMARY_TIER_PREFIX);
        properties.setProperty(Joiner.on(".").join(primaryTierPrefix, Location.BLOB_STORE_ID_FIELD), "0");
        properties.setProperty(Joiner.on(".").join(primaryTierPrefix, Location.CONTAINER_NAME_FIELD), "container");
        String cacheTierPrefix = Joiner.on(".").join(virtualContainerPrefix, VirtualContainer.CACHE_TIER_PREFIX);
        properties.setProperty(Joiner.on(".").join(cacheTierPrefix, Location.BLOB_STORE_ID_FIELD), "0");
        properties.setProperty(Joiner.on(".").join(cacheTierPrefix, Location.CONTAINER_NAME_FIELD), "cache");
        properties.setProperty(Joiner.on(".").join(cacheTierPrefix, "policy"), "WriteBackPolicy");
        properties.setProperty(Joiner.on(".").join(cacheTierPrefix, WriteBackPolicy.EVICT_DELAY), "P0D");
        properties.setProperty(Joiner.on(".").join(cacheTierPrefix, WriteBackPolicy.COPY_DELAY), "P0D");
        properties.setProperty("bounce.backends", "0");
        properties.setProperty("bounce.containers", "0");
        String backendPrefix = Joiner.on(".").join(BounceBlobStore.STORE_PROPERTY, "0");
        properties.setProperty(Joiner.on(".").join(backendPrefix, "jclouds.provider"), "transient");
        properties.setProperty(Joiner.on(".").join(backendPrefix, "nickname"), "memory");
        properties.setProperty("s3proxy.endpoint", "http://127.0.0.1:0");
        properties.setProperty("s3proxy.authorization", "aws-v2");
        properties.setProperty("jclouds.provider", "bounce");
        return properties;
    }
}
