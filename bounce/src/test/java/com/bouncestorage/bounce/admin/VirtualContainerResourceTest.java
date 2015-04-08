/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import com.bouncestorage.bounce.admin.policy.WriteBackPolicy;
import com.google.common.base.Joiner;

import org.apache.commons.configuration.Configuration;
import org.junit.Before;
import org.junit.Test;

public class VirtualContainerResourceTest {
    private BounceApplication app;

    @Before
    public void setUp() throws Exception {
        app = new BounceApplication();
        Properties properties = new Properties();
        try (InputStream is = VirtualContainerResourceTest.class.getResourceAsStream("/example.properties")) {
            properties.load(is);
        }
        app.useRandomPorts();
        app.getConfiguration().setAll(properties);
        String webConfig = VirtualContainerResourceTest.class.getResource("/bounce.yml").toExternalForm();
        synchronized (app.getClass()) {
            app.run(new String[]{"server", webConfig});
        }
    }

    @Test
    public void testVirtualContainerPut() throws Exception {
        String jsonInput = "{\"cacheLocation\":{\"blobStoreId\":0,\"containerName\":\"other\"," +
                "\"copyDelay\":\"P0D\",\"moveDelay\":\"P1D\"},\"originLocation\":{\"blobStoreId\":1," +
                "\"containerName\":\"test\",\"copyDelay\":\"P90D\",\"moveDelay\":\"P180D\"}," +
                "\"archiveLocation\":{\"blobStoreId\":0,\"containerName\":\"archive\",\"copyDelay\":null," +
                "\"moveDelay\":null},\"migrationTargetLocation\":{\"blobStoreId\":-1,\"containerName\":\"\"," +
                "\"copyDelay\":null,\"moveDelay\":null},\"name\":\"magic\"}";


        String url = String.format("http://localhost:%d/api/virtual_container/0", app.getPort());
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestMethod("PUT");
        connection.setRequestProperty(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
        connection.setDoOutput(true);
        try (OutputStream os = connection.getOutputStream()) {
            os.write(jsonInput.getBytes(StandardCharsets.UTF_8));
        }

        String response = connection.getResponseMessage();
        assertThat(response.equalsIgnoreCase("OK"));
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
}
