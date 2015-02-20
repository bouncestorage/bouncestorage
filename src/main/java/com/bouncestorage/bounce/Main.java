/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import static com.google.common.base.Throwables.propagate;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

import com.bouncestorage.bounce.admin.BounceApplication;
import com.google.common.base.Optional;
import com.google.common.base.Strings;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.MapConfiguration;
import org.gaul.s3proxy.S3Proxy;
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

        final String localIdentity;
        final String localCredential;
        if (s3ProxyAuthorization.equalsIgnoreCase("aws-v2")) {
            localIdentity = properties.getProperty(
                    S3ProxyConstants.PROPERTY_IDENTITY);
            localCredential = properties.getProperty(
                    S3ProxyConstants.PROPERTY_CREDENTIAL);
            if (localIdentity == null || localCredential == null) {
                logger.error("Both {} and {} must be set",
                        S3ProxyConstants.PROPERTY_IDENTITY,
                        S3ProxyConstants.PROPERTY_CREDENTIAL);
                System.exit(1);
            }
        } else if (!s3ProxyAuthorization.equalsIgnoreCase("none")) {
            logger.error("{} must be aws-v2 or none, was: ",
                    S3ProxyConstants.PROPERTY_AUTHORIZATION,
                    s3ProxyAuthorization);
            System.exit(1);
            localIdentity = null;
            localCredential = null;
        } else {
            localIdentity = null;
            localCredential = null;
        }

        String keyStorePath = properties.getProperty(
                S3ProxyConstants.PROPERTY_KEYSTORE_PATH);
        String keyStorePassword = properties.getProperty(
                S3ProxyConstants.PROPERTY_KEYSTORE_PASSWORD);
        if (s3ProxyEndpointString.startsWith("https")) {
            if (Strings.isNullOrEmpty(keyStorePath) ||
                    Strings.isNullOrEmpty(keyStorePassword)) {
                logger.error("Both {} and {} must be set with an HTTPS endpoint",
                        S3ProxyConstants.PROPERTY_KEYSTORE_PATH,
                        S3ProxyConstants.PROPERTY_KEYSTORE_PASSWORD);
                System.exit(1);
            }
        }


        String forceMultiPartUpload = properties.getProperty(
                S3ProxyConstants.PROPERTY_FORCE_MULTI_PART_UPLOAD);
        Optional<String> virtualHost = Optional.fromNullable(
                properties.getProperty(
                        S3ProxyConstants.PROPERTY_VIRTUAL_HOST));

        URI s3ProxyEndpoint = new URI(s3ProxyEndpointString);

        AbstractConfiguration config = new MapConfiguration((Map) properties);
        BounceApplication app = new BounceApplication(config);
        String webConfig = Main.class.getResource("/bounce.yml").toExternalForm();
        app.run(new String[] {"server", webConfig});
        app.addBlobStoreListener(context -> {
            S3Proxy s3Proxy = new S3Proxy(context.getBlobStore(), s3ProxyEndpoint,
                    localIdentity, localCredential, keyStorePath,
                    keyStorePassword,
                    "true".equalsIgnoreCase(forceMultiPartUpload), virtualHost);

            try {
                s3Proxy.start();
            } catch (Exception e) {
                throw propagate(e);
            }
        });
    }
}
