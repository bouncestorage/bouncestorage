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
import java.util.Properties;

import com.bouncestorage.bounce.admin.BounceApplication;
import com.bouncestorage.bounce.admin.ConfigurationResource;
import com.google.common.base.Optional;
import com.google.common.base.Strings;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.MapConfiguration;
import org.gaul.s3proxy.S3Proxy;
import org.gaul.s3proxy.S3ProxyConstants;

public final class Main {
    /* hide useless constructor */
    private Main() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals("--version")) {
            System.err.println(
                    Main.class.getPackage().getImplementationVersion());
            System.exit(0);
        } else if (args.length != 2) {
            System.err.println("Usage: bounce --properties FILE");
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
            System.err.println("Properties file must contain:\n" +
                    S3ProxyConstants.PROPERTY_ENDPOINT + "\n" +
                    S3ProxyConstants.PROPERTY_AUTHORIZATION + "\n"
            );
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
                System.err.println(
                        "Both " + S3ProxyConstants.PROPERTY_IDENTITY +
                                " and " + S3ProxyConstants.PROPERTY_CREDENTIAL +
                                " must be set");
                System.exit(1);
            }
        } else if (!s3ProxyAuthorization.equalsIgnoreCase("none")) {
            System.err.println(S3ProxyConstants.PROPERTY_AUTHORIZATION +
                    " must be aws-v2 or none, was: " + s3ProxyAuthorization);
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
                System.err.println(
                        "Both " + S3ProxyConstants.PROPERTY_KEYSTORE_PATH +
                                " and " + S3ProxyConstants.PROPERTY_KEYSTORE_PASSWORD +
                                " must be set with an HTTP endpoint");
                System.exit(1);
            }
        }


        String forceMultiPartUpload = properties.getProperty(
                S3ProxyConstants.PROPERTY_FORCE_MULTI_PART_UPLOAD);
        Optional<String> virtualHost = Optional.fromNullable(
                properties.getProperty(
                        S3ProxyConstants.PROPERTY_VIRTUAL_HOST));

        URI s3ProxyEndpoint = new URI(s3ProxyEndpointString);

        AbstractConfiguration config = new MapConfiguration(properties);
        ConfigurationResource backendConfig =
                new ConfigurationResource(config);
        BounceApplication app = new BounceApplication(backendConfig);
        String webConfig = Main.class.getResource("/bounce.yml").toExternalForm();
        app.run(new String[] {"server", webConfig});
        backendConfig.addBlobStoreListener(context -> {
            BounceBlobStore bounceStore = (BounceBlobStore) context.getBlobStore();

            S3Proxy s3Proxy = new S3Proxy(context.getBlobStore(), s3ProxyEndpoint,
                    localIdentity, localCredential, keyStorePath,
                    keyStorePassword,
                    "true".equalsIgnoreCase(forceMultiPartUpload), virtualHost);

            app.useBlobStore(bounceStore);
            try {
                s3Proxy.start();
            } catch (Exception e) {
                throw propagate(e);
            }
        });
        backendConfig.init();
    }
}
