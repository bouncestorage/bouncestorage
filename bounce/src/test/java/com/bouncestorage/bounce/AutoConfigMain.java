/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;

import com.bouncestorage.bounce.admin.ObjectStoreResourceTest;
import com.bouncestorage.bounce.utils.BounceAutoConfigApplication;
import com.google.common.io.ByteStreams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AutoConfigMain {
    private static Logger logger = LoggerFactory.getLogger(Main.class);

    /* hide useless constructor */
    private AutoConfigMain() {
    }

    public static void main(String[] args) throws Exception {
        File configFile = File.createTempFile("bounce-test", "properties");
        try (FileOutputStream out = new FileOutputStream(configFile);
             InputStream is = ObjectStoreResourceTest.class.getResourceAsStream("/bounce.properties")) {
            ByteStreams.copy(is, out);
        }
        BounceAutoConfigApplication app = new BounceAutoConfigApplication(configFile.getPath());
        String webConfig = Main.class.getResource("/bounce.yml").toExternalForm();
        app.run(new String[] {"server", webConfig});
    }
}
