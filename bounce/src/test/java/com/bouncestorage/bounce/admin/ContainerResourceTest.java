/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;

import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.policy.WriteBackPolicy;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ContainerResourceTest {
    private static final String createObjectStoreRequest =
            "{\"provider\":\"transient\", \"nickname\":\"foo\", \"identity\":\"foo\", \"credential\":\"bar\"}";

    private BounceApplication app;
    private File configFile;
    private String apiURL;
    private String containerAPI;
    private int objectStoreId;

    @Before
    public void setUp() throws Exception {
        configFile = File.createTempFile("bounce-test", "properties");
        try (FileOutputStream out = new FileOutputStream(configFile);
             InputStream is = ObjectStoreResourceTest.class.getResourceAsStream("/bounce.properties")) {
            ByteStreams.copy(is, out);
        }
        app = new BounceApplication(configFile.getPath());
        app.useRandomPorts();
        String webConfig = ObjectStoreResourceTest.class.getResource("/bounce.yml").toExternalForm();
        synchronized (app.getClass()) {
            app.run("server", webConfig);
        }
        apiURL = String.format("http://localhost:%d/api/object_store", app.getPort());

        HttpURLConnection connection = UtilsTest.submitRequest(apiURL, HttpMethod.POST, createObjectStoreRequest);
        assertThat(connection.getResponseCode()).isEqualTo(Response.Status.OK.getStatusCode());
        try (InputStream stream = connection.getInputStream()) {
            Map<String, Object> map = new ObjectMapper().readValue(stream, new TypeReference<Map<String, Object>>() {
            });
            containerAPI = apiURL + "/" + map.get("id") + "/container";
            objectStoreId = (Integer) map.get("id");
        }
    }

    @After
    public void tearDown() throws Exception {
        app.stop();
        if (configFile != null) {
            configFile.delete();
        }
    }

    @Test
    public void testCreateContainer() throws Exception {
        String containerName = UtilsTest.createRandomContainerName();
        String createContainerJSON = String.format("{\"name\":\"%s\"}", containerName);
        HttpURLConnection connection = UtilsTest.submitRequest(containerAPI, HttpMethod.POST, createContainerJSON);
        assertThat(connection.getResponseCode()).isEqualTo(Response.Status.OK.getStatusCode());
        assertThat(app.getBlobStore(objectStoreId).containerExists(containerName)).isTrue();
    }

    @Test
    public void testListContainer() throws Exception {
        String containerName = UtilsTest.createRandomContainerName();
        String blobName = UtilsTest.createRandomBlobName();
        BlobStore blobStore = app.getBlobStore(objectStoreId);
        blobStore.createContainerInLocation(null, containerName);
        Blob blob = UtilsTest.makeBlob(blobStore, blobName);
        blobStore.putBlob(containerName, blob);

        HttpURLConnection connection = UtilsTest.submitRequest(containerAPI + "/" + containerName,
                HttpMethod.GET, null);
        assertThat(connection.getResponseCode()).isEqualTo(Response.Status.OK.getStatusCode());
        try (InputStream stream = connection.getInputStream()) {
            Map<String, Object> map = new ObjectMapper().readValue(stream, new TypeReference<Map<String, Object>>() {
            });
            assertThat(map).containsKey("objects");
            assertThat(map.get("objects")).isInstanceOf(List.class);
            List objects = (List) map.get("objects");
            assertThat(objects).hasSize(1);
            assertThat(((Map<String, String>) objects.get(0)).get("name")).isEqualTo(blobName);
        }
    }

    @Test
    public void testListWithVirtualContainer() throws Exception {
        String cacheContainer = UtilsTest.createRandomContainerName();
        String originContainer = UtilsTest.createRandomContainerName();
        String virtualContainerName = UtilsTest.createRandomContainerName();
        BlobStore blobStore = app.getBlobStore(objectStoreId);
        blobStore.createContainerInLocation(null, originContainer);
        blobStore.createContainerInLocation(null, cacheContainer);
        Properties newProperties = new Properties();
        String prefix = VirtualContainerResource.VIRTUAL_CONTAINER_PREFIX + ".0";
        Map<String, String> settings = new ImmutableMap.Builder<String, String>()
                .put(prefix + ".name", virtualContainerName)
                .put(Joiner.on(".").join(prefix, VirtualContainer.CACHE_TIER_PREFIX, Location.BLOB_STORE_ID_FIELD),
                        Integer.toString(objectStoreId))
                .put(Joiner.on(".").join(prefix, VirtualContainer.CACHE_TIER_PREFIX, Location.CONTAINER_NAME_FIELD),
                        cacheContainer)
                .put(Joiner.on(".").join(prefix, VirtualContainer.CACHE_TIER_PREFIX, "policy"),
                        WriteBackPolicy.class.getSimpleName())
                .put(Joiner.on(".").join(prefix, VirtualContainer.CACHE_TIER_PREFIX, "evictDelay"),
                        Duration.ofHours(1).toString())
                .put(Joiner.on(".").join(prefix, VirtualContainer.CACHE_TIER_PREFIX, "copyDelay"),
                        Duration.ofHours(0).toString())
                .put(Joiner.on(".").join(prefix, VirtualContainer.PRIMARY_TIER_PREFIX, Location.BLOB_STORE_ID_FIELD),
                        Integer.toString(objectStoreId))
                .put(Joiner.on(".").join(prefix, VirtualContainer.PRIMARY_TIER_PREFIX, Location.CONTAINER_NAME_FIELD),
                        originContainer)
                .build();
        newProperties.putAll(settings);
        new ConfigurationResource(app).updateConfig(newProperties);
        String blobName = UtilsTest.createRandomBlobName();
        BlobStore vBlobStore = app.getBlobStore(virtualContainerName);
        Blob blob = UtilsTest.makeBlob(vBlobStore, blobName);
        vBlobStore.putBlob(virtualContainerName, blob);

        HttpURLConnection connection = UtilsTest.submitRequest(containerAPI + "/" + virtualContainerName,
                HttpMethod.GET, null);
        assertThat(connection.getResponseCode()).isEqualTo(Response.Status.OK.getStatusCode());
        try (InputStream stream = connection.getInputStream()) {
            Map<String, Object> map = new ObjectMapper().readValue(stream, new TypeReference<Map<String, Object>>() {
            });
            assertThat(map).containsKey("objects");
            assertThat(map.get("objects")).isInstanceOf(List.class);
            List objects = (List) map.get("objects");
            assertThat(objects).hasSize(1);
            assertThat(((Map<String, String>) objects.get(0)).get("name")).isEqualTo(blobName);
        }
    }
}
