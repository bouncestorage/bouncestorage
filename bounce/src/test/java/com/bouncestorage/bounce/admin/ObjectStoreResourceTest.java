/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;

import javax.ws.rs.HttpMethod;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.UtilsTest;
import com.google.common.io.ByteStreams;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ObjectStoreResourceTest {
    private BounceApplication app;
    private File configFile;

    @Before
    public void setUp() throws Exception {
        configFile = File.createTempFile("bounce-test", "properties");
        try (FileOutputStream out = new FileOutputStream(configFile);
             InputStream is = ObjectStoreResourceTest.class.getResourceAsStream("/bounce.properties")) {
            ByteStreams.copy(is, out);
        }
        app = new BounceApplication(configFile.getPath());
        app.useRandomPorts();
        String webConfig = VirtualContainerResourceTest.class.getResource("/bounce.yml").toExternalForm();
        synchronized (app.getClass()) {
            app.run(new String[]{"server", webConfig});
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
    public void testObjectStorePost() throws Exception {
        String[] nicknames = {"foo"};
        validateStores(nicknames);
    }

    @Test
    public void testTwoObjectStorePost() throws Exception {
        String[] nicknames = {"foo", "bar"};
        validateStores(nicknames);
    }

    @Test
    public void testThreeObjectStorePost() throws Exception {
        String[] nicknames = {"foo", "bar", "baz"};
        validateStores(nicknames);
    }

    private void validateStores(String[] nicknames) throws Exception {
        for (int i = 0; i < nicknames.length; i++) {
            String response = createObjectStore(nicknames[i]);
            assertThat(response).isEqualToIgnoringCase("OK");
            Configuration config = app.getConfiguration();
            assertThat(config.getList(BounceBlobStore.STORES_LIST)).hasSize(i + 1);
            String id = Integer.toString(i + 1);
            String key = BounceBlobStore.STORE_PROPERTY + "." + id + ".jclouds.nickname";
            assertThat(config.getString(key)).isEqualTo(nicknames[i]);
            PropertiesConfiguration savedConfig = new PropertiesConfiguration(configFile);
            assertThat(savedConfig.getString(key)).isEqualTo(config.getString(key));
        }
    }

    private String createObjectStore(String nickname) throws Exception {
        String jsonInput = "{ \"provider\" : \"transient\", \"identity\" : \"foo\", \"credential\" : \"foo\", " +
                "\"nickname\" : \"" + nickname + "\" }";

        String url = String.format("http://localhost:%d/api/object_store", app.getPort());
        return UtilsTest.submitRequest(url, HttpMethod.POST, jsonInput);
    }
}
