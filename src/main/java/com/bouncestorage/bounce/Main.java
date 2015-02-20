/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import com.bouncestorage.bounce.admin.BounceApplication;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.MapConfiguration;
import org.gaul.s3proxy.S3ProxyConstants;
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
        Properties properties = new Properties();
        try (InputStream is = new FileInputStream(new File(args[1]))) {
            properties.load(is);
        }

        String s3ProxyEndpointString = properties.getProperty(
                S3ProxyConstants.PROPERTY_ENDPOINT);
        String s3ProxyAuthorization = properties.getProperty(
                S3ProxyConstants.PROPERTY_AUTHORIZATION);
        if (s3ProxyEndpointString == null ||
                s3ProxyAuthorization == null) {
            logger.error("Properties file must contain: {} {}",
                    S3ProxyConstants.PROPERTY_ENDPOINT,
                    S3ProxyConstants.PROPERTY_AUTHORIZATION);
            System.exit(1);
        }

        if (!(s3ProxyAuthorization.equalsIgnoreCase("aws-v2") ||
                s3ProxyAuthorization.equalsIgnoreCase("none"))) {
            logger.error("{} must be aws-v2 or none, was: {}",
                    S3ProxyConstants.PROPERTY_AUTHORIZATION,
                    s3ProxyAuthorization);
            System.exit(1);
        }

        AbstractConfiguration config = new MapConfiguration((Map) properties);
        BounceApplication app = new BounceApplication(config);
        String webConfig = Main.class.getResource("/bounce.yml").toExternalForm();
        app.run(new String[] {"server", webConfig});
    }
}
