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
import java.util.Map;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;

import com.bouncestorage.bounce.UtilsTest;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;

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
            app.run(new String[]{"server", webConfig});
        }
        apiURL = String.format("http://localhost:%d/api/object_store", app.getPort());

        HttpURLConnection connection = UtilsTest.submitRequest(apiURL, HttpMethod.POST, createObjectStoreRequest);
        assertThat(connection.getResponseCode()).isEqualTo(Response.Status.OK.getStatusCode());
        try (InputStream stream = connection.getInputStream()) {
            Map<String, Object> map = new ObjectMapper().readValue(stream, new TypeReference<Map<String, Object>>() {
            });
            containerAPI = apiURL + "/" + map.get("id") + "/container";
            objectStoreId = Integer.valueOf((Integer) map.get("id"));
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
}
