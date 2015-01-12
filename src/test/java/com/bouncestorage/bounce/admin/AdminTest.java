/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.Properties;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.UtilsTest;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;

import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.http.HttpException;
import org.jclouds.rest.HttpClient;
import org.jclouds.util.Strings2;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public final class AdminTest {
    private static final String ADMIN_ENDPOINT = "http://localhost:9000/";
    private BlobStoreContext bounceContext;
    private BounceBlobStore bounceBlobStore;
    private String containerName;
    private HttpClient httpClient;

    @Before
    public void setUp() throws Exception {
        containerName = UtilsTest.createRandomContainerName();

        Properties nearProperties = new Properties();
        nearProperties.putAll(ImmutableMap.of(
                Constants.PROPERTY_PROVIDER, "transient"
        ));

        Properties farProperties = new Properties();
        farProperties.putAll(ImmutableMap.of(
                Constants.PROPERTY_PROVIDER, "transient"
        ));

        Properties dummy = new Properties();
        dummy.putAll(ImmutableMap.of(
                BounceBlobStore.STORE_PROPERTY_1, "",
                BounceBlobStore.STORE_PROPERTY_2, ""
        ));
        bounceContext = ContextBuilder
                .newBuilder("bounce")
                .overrides(dummy)
                .build(BlobStoreContext.class);
        httpClient = bounceContext.utils().http();

        bounceBlobStore = (BounceBlobStore) bounceContext.getBlobStore();
        bounceBlobStore.initStores(nearProperties, farProperties);
        bounceBlobStore.createContainerInLocation(null, containerName);
        String config = getClass().getResource("/bounce.yml").toExternalForm();
        new BounceApplication(bounceBlobStore).run(new String[] {
            "server", config });
    }

    @After
    public void tearDown() throws Exception {
        if (bounceBlobStore != null) {
            bounceBlobStore.deleteContainer(containerName);
        }
        if (bounceContext != null) {
            bounceContext.close();
        }
    }

    // TODO: how to stop DropWizard to re-use port?
    @Ignore
    @Test
    public void testServiceResource() throws Exception {
        String output = Strings2.toStringAndClose(httpClient.get(URI.create(
                ADMIN_ENDPOINT + "service")));
        assertThat(output).isEqualTo("{\"containerNames\":[\"" +
                containerName + "\"]}");
    }

    // TODO: how to stop DropWizard to re-use port?
    @Ignore
    @Test
    public void testContainerResource() throws Exception {
        String output = Strings2.toStringAndClose(httpClient.get(URI.create(
                ADMIN_ENDPOINT + "container?name=" + containerName)));
        assertThat(output).isEqualTo(
                "{\"blobNames\":[],\"bounceLinkCount\":0}");
    }

    @Test
    public void testBounceBlobsResource() throws Exception {
        String blobName = "blob";
        ByteSource byteSource = ByteSource.wrap(new byte[0]);
        Blob blob = bounceBlobStore.blobBuilder(blobName)
                .payload(byteSource)
                .contentLength(byteSource.size())
                .build();
        bounceBlobStore.putBlob(containerName, blob);

        String output = Strings2.toStringAndClose(httpClient.get(URI.create(
                ADMIN_ENDPOINT + "container?name=" + containerName)));
        assertThat(output).isEqualTo(
                "{\"blobNames\":[\"" + blobName + "\"],\"bounceLinkCount\":0}");

        try {
            httpClient.post(
                    URI.create(ADMIN_ENDPOINT + "bounce?name=" + containerName),
                    blob.getPayload());
        } catch (HttpException he) {
            // TODO: jclouds expects an ETag but DropWizard does not provide one
        }

        output = Strings2.toStringAndClose(httpClient.get(URI.create(
                ADMIN_ENDPOINT + "container?name=" + containerName)));
        assertThat(output).isEqualTo(
                "{\"blobNames\":[\"" + blobName + "\"],\"bounceLinkCount\":1}");
    }
}
