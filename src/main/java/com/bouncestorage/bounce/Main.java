/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;

import com.bouncestorage.bounce.admin.BounceApplication;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;

import org.gaul.s3proxy.S3Proxy;
import org.gaul.s3proxy.S3ProxyConstants;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;

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
        properties.putAll(System.getProperties());

        String s3ProxyEndpointString = properties.getProperty(
                S3ProxyConstants.PROPERTY_ENDPOINT);
        String s3ProxyAuthorization = properties.getProperty(
                S3ProxyConstants.PROPERTY_AUTHORIZATION);
        String nearStorePropertiesPath = properties.getProperty(
                BounceBlobStore.STORE_PROPERTY_1);
        String farStorePropertiesPath = properties.getProperty(
                BounceBlobStore.STORE_PROPERTY_2);
        if (s3ProxyEndpointString == null ||
                s3ProxyAuthorization == null ||
                nearStorePropertiesPath == null ||
                farStorePropertiesPath == null) {
            System.err.println("Properties file must contain:\n" +
                    S3ProxyConstants.PROPERTY_ENDPOINT + "\n" +
                    S3ProxyConstants.PROPERTY_AUTHORIZATION + "\n" +
                    BounceBlobStore.STORE_PROPERTY_1 + "\n" +
                    BounceBlobStore.STORE_PROPERTY_2 + "\n"
            );
            System.exit(1);
        }

        String localIdentity = null;
        String localCredential = null;
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

        ContextBuilder builder = ContextBuilder
                .newBuilder("bounce")
                .modules(ImmutableList.<Module>of(new SLF4JLoggingModule()));
        BlobStoreContext context = builder.build(BlobStoreContext.class);
        BounceBlobStore bounceStore = (BounceBlobStore) context.getBlobStore();
        File nearStoreFile = new File(nearStorePropertiesPath);
        File farStoreFile = new File(farStorePropertiesPath);
        Properties nearStoreProps = Utils.propertiesFromFile(nearStoreFile);
        Properties farStoreProps = Utils.propertiesFromFile(farStoreFile);
        bounceStore.initStores(nearStoreProps, farStoreProps);
        URI s3ProxyEndpoint = new URI(s3ProxyEndpointString);
        S3Proxy s3Proxy = new S3Proxy(context.getBlobStore(), s3ProxyEndpoint,
                localIdentity, localCredential, keyStorePath,
                keyStorePassword,
                "true".equalsIgnoreCase(forceMultiPartUpload), virtualHost);

        new BounceApplication(bounceStore).run(new String[] {
            "server", "src/main/resources/bounce.yml" });
        s3Proxy.start();
    }
}
