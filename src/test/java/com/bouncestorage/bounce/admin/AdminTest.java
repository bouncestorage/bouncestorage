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
import com.bouncestorage.bounce.admin.policy.BounceEverythingPolicy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;

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
    private static final String ADMIN_ENDPOINT = "http://localhost";
    private BlobStoreContext bounceContext;
    private BounceBlobStore bounceBlobStore;
    private String containerName;
    private HttpClient httpClient;
    private BounceApplication app;

    @Before
    public void setUp() throws Exception {
        containerName = UtilsTest.createRandomContainerName();

        bounceContext = UtilsTest.createTransientBounceBlobStore();
        httpClient = bounceContext.utils().http();

        bounceBlobStore = (BounceBlobStore) bounceContext.getBlobStore();
        bounceBlobStore.createContainerInLocation(null, containerName);


        String config = getClass().getResource("/bounce.yml").toExternalForm();
        app = new BounceApplication(new ConfigurationResource(new Properties()));
        app.useRandomPorts();
        app.run(new String[] {
                "server", config
        });
        app.useBlobStore(bounceBlobStore);
        BounceService bounceService = app.getBounceService();
        bounceService.installPolicies(ImmutableList.of(
                new BounceEverythingPolicy()
        ));
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
        String output = Strings2.toStringAndClose(httpClient.get(createURI("service")));
        assertThat(output).isEqualTo("{\"containerNames\":[\"" +
                containerName + "\"]}");
    }

    // TODO: how to stop DropWizard to re-use port?
    @Ignore
    @Test
    public void testContainerResource() throws Exception {
        String output = Strings2.toStringAndClose(httpClient.get(createURI(
                "container?name=" + containerName)));
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

        String output = Strings2.toStringAndClose(httpClient.get(createURI(
                "container?name=" + containerName)));
        assertThat(output).isEqualTo(
                "{\"blobNames\":[\"" + blobName + "\"],\"bounceLinkCount\":0}");

        try {
            httpClient.post(
                    createURI("bounce?name=" + containerName + "&" + "wait=true"),
                    blob.getPayload());
        } catch (HttpException he) {
            // TODO: jclouds expects an ETag but DropWizard does not provide one
        }

        output = Strings2.toStringAndClose(httpClient.get(createURI(
                "container?name=" + containerName)));
        assertThat(output).isEqualTo(
                "{\"blobNames\":[\"" + blobName + "\"],\"bounceLinkCount\":1}");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode status = mapper.readTree(httpClient.get(createURI("bounce")));
        assertThat(status).hasSize(1);
        status = status.get(0);
        assertThat(status.get("totalObjectCount").asLong()).isEqualTo(1);
        assertThat(status.get("bouncedObjectCount").asLong()).isEqualTo(1);
        assertThat(status.get("errorObjectCount").asLong()).isEqualTo(0);
        assertThat(status.get("done").asBoolean()).isEqualTo(true);
    }

    private URI createURI(String uri) {
        return URI.create(ADMIN_ENDPOINT + ":" + app.getPort() + "/" + uri);
    }
}
