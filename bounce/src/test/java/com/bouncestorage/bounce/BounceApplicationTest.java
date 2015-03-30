/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import com.bouncestorage.bounce.admin.BounceApplication;
import com.google.common.collect.ImmutableList;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.MapConfiguration;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.gaul.s3proxy.S3ProxyConstants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class BounceApplicationTest {
    private Properties properties;
    private String webConfig = Main.class.getResource("/bounce.yml")
            .toExternalForm();
    private BounceApplication app;
    String IDENTITY;
    String CREDENTIAL;

    @Before
    public void setUp() throws Exception {
        properties = new Properties();
        try (InputStream is = BounceApplicationTest.class
                .getResourceAsStream("/bounce.properties")) {
            properties.load(is);
        }

        IDENTITY = properties.getProperty(S3ProxyConstants.PROPERTY_IDENTITY);
        CREDENTIAL = properties.getProperty(S3ProxyConstants.PROPERTY_CREDENTIAL);

        AbstractConfiguration config = new MapConfiguration((Map) properties);
        config.addProperty("bounce.backends", ImmutableList.of("0"));
        config.addProperty("bounce.backend.0.jclouds.provider", "transient");
        config.addProperty("bounce.backend.0.jclouds.identity", IDENTITY);
        config.addProperty("bounce.backend.0.jclouds.credential", CREDENTIAL);
        app = new BounceApplication(config);
        app.useRandomPorts();
        synchronized (BounceApplication.class) {
            app.run(new String[]{"server", webConfig});
        }
    }

    @After
    public void tearDown() throws Exception {
        app.stop();
    }

    @Test
    public void testS3ProxyStartup() throws Exception {
        while (!app.getS3ProxyState().equals(AbstractLifeCycle.STARTED)) {
            Thread.sleep(10);
        }

        BlobStoreContext context = ContextBuilder.newBuilder("s3")
                .endpoint("http://127.0.0.1:" + app.getS3ProxyPort())
                .credentials(properties.getProperty(S3ProxyConstants.PROPERTY_IDENTITY),
                        properties.getProperty(S3ProxyConstants.PROPERTY_CREDENTIAL))
                .build(BlobStoreContext.class);
        BlobStore blobStore = context.getBlobStore();
        PageSet<? extends StorageMetadata> res = blobStore.list();
        assertThat(res).isEmpty();
    }

    @Test
    public void testConfigureProviders() throws Exception {
        Map.Entry<String, BlobStore> res = app.locateBlobStore(IDENTITY, null, null);
        assertThat(res.getKey()).isEqualTo(CREDENTIAL);
        assertThat(res.getValue()).isNotNull();
    }
}
