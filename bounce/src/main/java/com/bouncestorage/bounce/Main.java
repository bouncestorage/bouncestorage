/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import com.bouncestorage.bounce.admin.BounceApplication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Main {
    private static Logger logger = LoggerFactory.getLogger(Main.class);

    /* hide useless constructor */
    private Main() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals("--version")) {
            logger.error(Main.class.getPackage().getImplementationVersion());
            System.exit(0);
        } else if (args.length != 2) {
            logger.error("Usage: bounce --properties FILE");
            System.exit(1);
        }

        BounceApplication app = new BounceApplication(args[1]);
        String webConfig = Main.class.getResource("/bounce.yml").toExternalForm();
        app.run(new String[] {"server", webConfig});
    }
}
