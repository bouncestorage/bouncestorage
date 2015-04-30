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
import com.bouncestorage.bounce.admin.BounceConfiguration;
import com.bouncestorage.swiftproxy.SwiftProxy;

import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.gaul.s3proxy.S3ProxyConstants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.junit.After;
import org.junit.Test;

public final class BounceApplicationTest {
    private String webConfig = Main.class.getResource("/bounce.yml")
            .toExternalForm();
    private BounceApplication app;

    @After
    public void tearDown() throws Exception {
        if (app != null) {
            app.stop();
        }
    }

    @Test
    public void testS3ProxyStartup() throws Exception {
        initializeDefaultProperties();
        startApp();

        String identity = app.getConfiguration().getString(S3ProxyConstants.PROPERTY_IDENTITY);
        String credential = app.getConfiguration().getString(S3ProxyConstants.PROPERTY_CREDENTIAL);
        configureBlobStore(identity, credential);
        BlobStoreContext context = ContextBuilder.newBuilder("s3")
                .endpoint("http://127.0.0.1:" + app.getS3ProxyPort())
                .credentials(app.getConfiguration().getString(S3ProxyConstants.PROPERTY_IDENTITY),
                        app.getConfiguration().getString(S3ProxyConstants.PROPERTY_CREDENTIAL))
                .build(BlobStoreContext.class);
        BlobStore blobStore = context.getBlobStore();
        PageSet<? extends StorageMetadata> res = blobStore.list();
        assertThat(res).isEmpty();
    }

    @Test
    public void testSwiftProxyStartup() throws Exception {
        app = new BounceApplication();
        BounceConfiguration config = app.getConfiguration();
        config.setProperty(SwiftProxy.PROPERTY_ENDPOINT, "http://127.0.0.1:0");
        startApp();

        String identity = "foo";
        String credential = "bar";
        configureBlobStore(identity, credential);
        Properties swiftProperties = new Properties();
        swiftProperties.setProperty("jclouds.keystone.credential-type", "tempAuthCredentials");
        BlobStoreContext context = ContextBuilder.newBuilder("openstack-swift")
                .endpoint("http://127.0.0.1:" + app.getSwiftPort() + "/auth/v1.0")
                .overrides(swiftProperties)
                .credentials("foo", "bar")
                .build(BlobStoreContext.class);
        BlobStore blobStore = context.getBlobStore();
        PageSet<? extends StorageMetadata> res = blobStore.list();
        assertThat(res).isEmpty();
    }

    @Test
    public void testConfigureProviders() throws Exception {
        initializeDefaultProperties();
        startApp();
        String identity = app.getConfiguration().getString(S3ProxyConstants.PROPERTY_IDENTITY);
        String credential = app.getConfiguration().getString(S3ProxyConstants.PROPERTY_CREDENTIAL);
        configureBlobStore(identity, credential);
        Map.Entry<String, BlobStore> res = app.locateBlobStore(identity, null, null);
        assertThat(res.getKey()).isEqualTo(credential);
        assertThat(res.getValue()).isNotNull();
    }

    private void configureBlobStore(String identity, String credential) {
        BounceConfiguration config = app.getConfiguration();
        Properties properties = new Properties();
        properties.setProperty("bounce.backends", "0");
        properties.setProperty("bounce.backend.0.jclouds.provider", "transient");
        properties.setProperty("bounce.backend.0.jclouds.identity", identity);
        properties.setProperty("bounce.backend.0.jclouds.credential", credential);
        config.setAll(properties);
    }

    private void initializeDefaultProperties() throws Exception {
        Properties properties = new Properties();
        try (InputStream is = BounceApplicationTest.class.getResourceAsStream("/bounce.properties")) {
            properties.load(is);
        }
        app = new BounceApplication();
        app.getConfiguration().setAll(properties);
    }

    private void startApp() throws Exception {
        app.useRandomPorts();
        synchronized (BounceApplication.class) {
            app.run(new String[]{"server", webConfig});
        }
        while (!app.getS3ProxyState().equals(AbstractLifeCycle.STARTED) && !app.isSwiftProxyStarted()) {
            Thread.sleep(10);
        }
    }
}
