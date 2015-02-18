/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import static com.google.common.base.Throwables.propagate;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
import org.jclouds.blobstore.BlobStore;
import org.jclouds.logging.Logger;

public final class Main {
    private static Logger logger = Logger.NULL;

    private Properties properties;
    private S3Proxy s3Proxy;

    private BounceApplication app;
    private final URI s3ProxyEndpoint;
    private final String s3ProxyAuthorization;
    private final String localIdentity;
    private final String localCredential;
    private final String keyStorePath;
    private final String keyStorePassword;
    private final String forceMultiPartUpload;
    private final Optional<String> virtualHost;

    public Main(File configurationFile) throws Exception {
        properties = loadConfiguration(configurationFile);

        String s3ProxyEndpointString = properties.getProperty(
                S3ProxyConstants.PROPERTY_ENDPOINT);
        s3ProxyAuthorization = properties.getProperty(
                S3ProxyConstants.PROPERTY_AUTHORIZATION);
        if (s3ProxyAuthorization == null || s3ProxyEndpointString == null) {
            logger.error("Properties file must contain:\n{}\n{}\n",
                    S3ProxyConstants.PROPERTY_ENDPOINT,
                    S3ProxyConstants.PROPERTY_AUTHORIZATION);
            System.exit(1);
        }

        if (isAuthorizationEnabled()) {
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
        } else {
            localIdentity = null;
            localCredential = null;
        }

        keyStorePath = properties.getProperty(
                S3ProxyConstants.PROPERTY_KEYSTORE_PATH);
        keyStorePassword = properties.getProperty(
                S3ProxyConstants.PROPERTY_KEYSTORE_PASSWORD);
        checkEndpointKeystore(s3ProxyEndpointString);
        forceMultiPartUpload = properties.getProperty(
                S3ProxyConstants.PROPERTY_FORCE_MULTI_PART_UPLOAD);
        virtualHost = Optional.fromNullable(properties.getProperty(
                S3ProxyConstants.PROPERTY_VIRTUAL_HOST));
        s3ProxyEndpoint = new URI(s3ProxyEndpointString);
    }

    public void start() throws Exception {
        AbstractConfiguration config = new MapConfiguration((Map) properties);
        app = new BounceApplication(config);
        String webConfig = Main.class.getResource("/bounce.yml").
                toExternalForm();
        app.addBlobStoreListener(context -> {
            try {
                startS3Proxy(context.getBlobStore());
            } catch (Exception e) {
                throw propagate(e);
            }
        });
        app.run(new String[] {"server", webConfig});
    }

    private void startS3Proxy(BlobStore store) throws Exception {
        if (s3Proxy != null) {
            s3Proxy.stop();
        }

        s3Proxy = new S3Proxy(store, s3ProxyEndpoint, localIdentity,
                localCredential, keyStorePath, keyStorePassword,
                "true".equalsIgnoreCase(forceMultiPartUpload), virtualHost);
        s3Proxy.start();
    }

    private void checkEndpointKeystore(String endpointString) {
        if (!endpointString.startsWith("https")) {
            return;
        }

        if (!Strings.isNullOrEmpty(keyStorePath) &&
                !Strings.isNullOrEmpty(keyStorePassword)) {
            return;
        }

        logger.error("Both {} and {} must be set with an https endpoint",
                S3ProxyConstants.PROPERTY_KEYSTORE_PATH,
                S3ProxyConstants.PROPERTY_KEYSTORE_PASSWORD);
        System.exit(1);
    }

    private boolean isAuthorizationEnabled() {
        if (s3ProxyAuthorization.equalsIgnoreCase("aws-v2")) {
            return true;
        } else if (s3ProxyAuthorization.equalsIgnoreCase("none")) {
            return false;
        }

        logger.error("{} must be aws-v2 or none, was: ",
                    S3ProxyConstants.PROPERTY_AUTHORIZATION,
                    s3ProxyAuthorization);
        System.exit(1);
        return false;
    }

    private Properties loadConfiguration(File configurationFile) throws Exception {
        Properties newProperties = new Properties();
        try (InputStream is = new FileInputStream(configurationFile)) {
            newProperties.load(is);
        } catch (IOException e) {
            throw propagate(e);
        }
        return newProperties;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals("--version")) {
            logger.error(Main.class.getPackage().getImplementationVersion());
            System.exit(0);
        } else if (args.length != 2) {
            logger.error("Usage: bounce --properties FILE");
            System.exit(1);
        }

        Main mainApp = new Main(new File(args[1]));
        mainApp.start();
    }
}
