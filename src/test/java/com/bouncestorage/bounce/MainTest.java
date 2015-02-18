/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public final class MainTest {
    private File propertiesFile;

    @Before
    public void setUp() {
        propertiesFile = new File(MainTest.class.
                getResource("/bounce.properties").getPath());
    }

    @Test
    public void testS3ProxyStart() throws Exception {
        Main app = new Main(propertiesFile);
        Properties props = app.getProperties();
        props.setProperty("bounce.store.properties.1.jclouds.provider",
                "transient");
        props.setProperty("bounce.store.properties.2.jclouds.provider",
                "transient");
        app.setProperties(props);
        app.start();
        assertThat(app.getS3Proxy()).isNotNull();
    }
}
