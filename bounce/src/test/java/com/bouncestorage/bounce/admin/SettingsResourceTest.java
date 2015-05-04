/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;

import com.bouncestorage.bounce.UtilsTest;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SettingsResourceTest {
    private static final String settingsEndpoint = "/api/settings";
    private BounceApplication app;

    @Before
    public void setUp() throws Exception {
        app = new BounceApplication();
        app.useRandomPorts();
        String webConfig = SettingsResourceTest.class.getResource("/bounce.yml").toExternalForm();
        synchronized (app.getClass()) {
            app.run(new String[]{"server", webConfig});
        }
    }

    @After
    public void tearDown() throws Exception {
        app.stop();
    }

    @Test
    public void testEmptySettings() throws Exception {
        HttpURLConnection response = UtilsTest.submitRequest(getURL(), HttpMethod.GET, null);
        assertThat(response.getResponseCode()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    @Test
    public void testConfigureS3Endpoint() throws Exception {
        String json = "{\"s3Address\":\"127.0.0.42\", \"s3Port\": 10042}";
        HttpURLConnection response = UtilsTest.submitRequest(getURL(), HttpMethod.POST, json);
        assertThat(response.getResponseCode()).isEqualTo(Response.Status.OK.getStatusCode());
        HashMap<String, String> settings = getSettings();
        assertThat(settings).containsKey("s3Address");
        assertThat(settings).containsKey("s3Port");
        assertThat(settings.get("s3Address")).isEqualTo("127.0.0.42");
        assertThat(settings.get("s3Port")).isEqualTo("10042");
    }

    @Test
    public void testConfigureSwiftEndpoint() throws Exception {
        String json = "{\"swiftAddress\":\"127.0.0.24\", \"swiftPort\": 10024}";
        HttpURLConnection response = UtilsTest.submitRequest(getURL(), HttpMethod.POST, json);
        assertThat(response.getResponseCode()).isEqualTo(Response.Status.OK.getStatusCode());
        HashMap<String, String> settings = getSettings();
        assertThat(settings).containsKey("swiftAddress");
        assertThat(settings).containsKey("swiftPort");
        assertThat(settings.get("swiftAddress")).isEqualTo("127.0.0.24");
        assertThat(settings.get("swiftPort")).isEqualTo("10024");
    }

    private HashMap<String, String> getSettings() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new URL(getURL()), new TypeReference<HashMap<String, String>>() { });
    }

    private String getURL() {
        return "http://localhost:" + app.getPort() + settingsEndpoint;
    }
}
