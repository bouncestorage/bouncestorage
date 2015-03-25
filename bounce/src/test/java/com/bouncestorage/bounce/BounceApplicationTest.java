/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Map;
import java.util.Properties;

import com.bouncestorage.bounce.admin.BounceApplication;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.MapConfiguration;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.jclouds.Constants;
import org.junit.Before;
import org.junit.Test;

public final class BounceApplicationTest {
    private Properties properties;
    private String webConfig = Main.class.getResource("/bounce.yml")
            .toExternalForm();

    @Before
    public void setUp() throws Exception {
        properties = new Properties();
        try (InputStream is = BounceApplicationTest.class
                .getResourceAsStream("/bounce.properties")) {
            properties.load(is);
        }
    }

    @Test
    public void testS3ProxyStartup() throws Exception {
        properties.setProperty(BounceBlobStore.STORE_PROPERTY_1 + "." +
                Constants.PROPERTY_PROVIDER, "transient");
        properties.setProperty(BounceBlobStore.STORE_PROPERTY_2 + "." +
                Constants.PROPERTY_PROVIDER, "transient");
        AbstractConfiguration config = new MapConfiguration((Map) properties);
        BounceApplication app = new BounceApplication(config);
        synchronized (BounceApplication.class) {
            app.run(new String[]{"server", webConfig});
        }

        while (!app.getS3ProxyState().equals(AbstractLifeCycle.STARTED)) {
            Thread.sleep(10);
        }
        Socket sock = new Socket(InetAddress.getLocalHost(),
                app.getS3ProxyPort());
        sock.close();
        app.stop();
    }
}
