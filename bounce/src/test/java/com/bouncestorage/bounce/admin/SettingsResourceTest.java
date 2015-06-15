/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;

import javax.ws.rs.core.Response;

import com.bouncestorage.bounce.UtilsTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SettingsResourceTest {
    private BounceApplication app;
    private SettingsResource resource;

    @Before
    public void setUp() throws Exception {
        app = UtilsTest.newBounceApplication();
        app.useRandomPorts();
        app.initTestingKeyStore();
        String webConfig = SettingsResourceTest.class.getResource("/bounce.yml").toExternalForm();
        synchronized (app.getClass()) {
            app.run(new String[]{"server", webConfig});
        }
        resource = new SettingsResource(app);
    }

    @After
    public void tearDown() throws Exception {
        app.stop();
    }

    @Test
    public void testEmptySettings() throws Exception {
        assertThat(resource.getSettings()).isNotNull();
    }

    @Test
    public void testConfigureS3Endpoint() throws Exception {
        Random r = new Random();
        SettingsResource.Settings settings = new SettingsResource.Settings();
        settings.s3Address = "127.0.0.42";
        settings.s3Port = r.nextInt(65536);
        settings.s3SSLAddress = "127.0.0.42";
        settings.s3SSLPort = r.nextInt(65536);
        settings.s3Domain = "s3.amazonaws.com";

        assertThat(resource.updateSettings(settings).getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
        SettingsResource.Settings updated = resource.getSettings();
        assertThat(updated.s3Address).isEqualTo(settings.s3Address);
        assertThat(updated.s3Port).isEqualTo(settings.s3Port);
        assertThat(updated.s3SSLAddress).isEqualTo(settings.s3SSLAddress);
        assertThat(updated.s3SSLPort).isEqualTo(settings.s3SSLPort);
        assertThat(updated.s3Domain).isEqualTo(settings.s3Domain);
        assertThat(updated.domainCertificate).isNotEmpty();
    }

    @Test
    public void testConfigureSwiftEndpoint() throws Exception {
        Random r = new Random();
        SettingsResource.Settings settings = new SettingsResource.Settings();
        settings.swiftAddress = "127.0.0.24";
        settings.swiftPort = r.nextInt(65536);
        assertThat(resource.updateSettings(settings).getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
        SettingsResource.Settings updated = resource.getSettings();
        assertThat(updated.swiftAddress).isEqualTo(settings.swiftAddress);
        assertThat(updated.swiftPort).isEqualTo(settings.swiftPort);
    }
}
