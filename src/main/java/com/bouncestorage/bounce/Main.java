/*
 *
 *  * Copyright 2015 Bounce Storage <info@bouncestorage.com>
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.bouncestorage.bounce;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import org.gaul.s3proxy.S3Proxy;
import org.gaul.s3proxy.S3ProxyConstants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals("--version")) {
            System.err.println(
                    Main.class.getPackage().getImplementationVersion());
            System.exit(0);
        } else if (args.length != 2) {
            System.err.println("Usage: s3proxy --properties FILE");
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
        if (s3ProxyEndpointString == null ||
                s3ProxyAuthorization == null) {
            System.err.println("Properties file must contain:\n" +
                    S3ProxyConstants.PROPERTY_ENDPOINT + "\n" +
                    S3ProxyConstants.PROPERTY_AUTHORIZATION);
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
                .modules(ImmutableList.<Module>of(new SLF4JLoggingModule()))
                .overrides(properties);
        BlobStoreContext context = builder.build(BlobStoreContext.class);
        URI s3ProxyEndpoint = new URI(s3ProxyEndpointString);
        S3Proxy s3Proxy = new S3Proxy(context.getBlobStore(), s3ProxyEndpoint,
                localIdentity, localCredential, keyStorePath,
                keyStorePassword,
                "true".equalsIgnoreCase(forceMultiPartUpload), virtualHost);

        s3Proxy.start();
    }
}
