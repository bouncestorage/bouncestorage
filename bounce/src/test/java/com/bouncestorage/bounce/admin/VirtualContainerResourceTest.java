/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.HttpURLConnection;
import java.util.Properties;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.policy.LRUStoragePolicy;
import com.bouncestorage.bounce.admin.policy.StoragePolicy;
import com.bouncestorage.bounce.admin.policy.WriteBackPolicy;
import com.google.common.base.Joiner;

import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class VirtualContainerResourceTest {
    private BounceApplication app;

    @Before
    public void setUp() throws Exception {
        app = new BounceApplication();
        app.useRandomPorts();
        app.getConfiguration().setAll(getDefaultProperties());
        String webConfig = VirtualContainerResourceTest.class.getResource("/bounce.yml").toExternalForm();
        synchronized (app.getClass()) {
            app.run(new String[]{"server", webConfig});
        }
        setDefaultContainerProperties();
    }

    @After
    public void tearDown() throws Exception {
        app.stop();
    }

    @Test
    public void testVirtualContainerPut() throws Exception {
        app.getBlobStore(0).createContainerInLocation(null, "archive");

        String jsonInput = "{\"cacheLocation\":{\"blobStoreId\":0,\"containerName\":\"cache\"," +
                "\"copyDelay\":\"P0D\",\"moveDelay\":\"P0D\"},\"originLocation\":{\"blobStoreId\":0," +
                "\"containerName\":\"container\",\"copyDelay\":\"P90D\",\"moveDelay\":\"P180D\"}," +
                "\"archiveLocation\":{\"blobStoreId\":0,\"containerName\":\"archive\",\"copyDelay\":null," +
                "\"moveDelay\":null},\"migrationTargetLocation\":{\"blobStoreId\":-1,\"containerName\":\"\"," +
                "\"copyDelay\":null,\"moveDelay\":null},\"name\":\"magic\"}";

        String url = String.format("http://localhost:%d/api/virtual_container/0", app.getPort());

        HttpURLConnection response = UtilsTest.submitRequest(url, HttpMethod.PUT, jsonInput);
        assertThat(response.getResponseCode()).isEqualTo(Response.Status.OK.getStatusCode());
        String containerPrefix = Joiner.on(".").join(VirtualContainerResource.VIRTUAL_CONTAINER_PREFIX, "0");
        String primaryTierPrefix = Joiner.on(".").join(containerPrefix, VirtualContainer.PRIMARY_TIER_PREFIX);
        Configuration config = app.getConfiguration();
        assertThat(config.getProperty(Joiner.on(".").join(primaryTierPrefix, WriteBackPolicy
                .EVICT_DELAY))).isEqualTo("P180D");
        String archivePrefix = Joiner.on(".").join(containerPrefix, VirtualContainer.ARCHIVE_TIER_PREFIX);
        assertThat(config.getProperty(Joiner.on(".").join(archivePrefix, Location
                .BLOB_STORE_ID_FIELD))).isEqualTo("0");
        assertThat(config.getProperty(Joiner.on(".").join(archivePrefix, Location.CONTAINER_NAME_FIELD)))
                .isEqualTo("archive");
    }

    @Test
    public void testEnablingMigration() throws Exception {
        app.getBlobStore(0).createContainerInLocation(null, "other");
        app.getBlobStore(0).createContainerInLocation(null, "target");
        String jsonInput = "{\"cacheLocation\":{\"blobStoreId\":0,\"containerName\":\"other\"," +
                "\"copyDelay\":\"P0D\",\"moveDelay\":\"P0D\"},\"originLocation\":{\"blobStoreId\":0," +
                "\"containerName\":\"container\",\"copyDelay\":null,\"moveDelay\":null}," +
                "\"archiveLocation\":{\"blobStoreId\":-1,\"containerName\":\"\",\"copyDelay\":null," +
                "\"moveDelay\":null},\"migrationTargetLocation\":{\"blobStoreId\":0,\"containerName\":\"target\"," +
                "\"copyDelay\":null,\"moveDelay\":null},\"name\":\"magic\"}";

        String url = String.format("http://localhost:%d/api/virtual_container/0", app.getPort());
        HttpURLConnection response = UtilsTest.submitRequest(url, HttpMethod.PUT, jsonInput);
        assertThat(response.getResponseCode()).isEqualTo(Response.Status.OK.getStatusCode());
        String containerPrefix = Joiner.on(".").join(VirtualContainerResource.VIRTUAL_CONTAINER_PREFIX, "0");
        String targetLocationPrefix = Joiner.on(".").join(containerPrefix, VirtualContainer.MIGRATION_TIER_PREFIX);
        Configuration config = app.getConfiguration();
        assertThat(config.getProperty(Joiner.on(".").join(targetLocationPrefix, Location.BLOB_STORE_ID_FIELD)))
                .isEqualTo("0");
        assertThat(config.getProperty(Joiner.on(".").join(targetLocationPrefix, Location.CONTAINER_NAME_FIELD)))
                .isEqualTo("target");
    }

    @Test
    public void testCapacityAndEviction() throws Exception {
        // Setting both capacity and eviction time should result in an error
        app.getBlobStore(0).createContainerInLocation(null, "archive");
        Long capacity = 50000L;
        VirtualContainer newContainer = new VirtualContainer();
        newContainer.setName("vBucket");
        Location origin = new Location();
        origin.setBlobStoreId(0);
        origin.setContainerName("container");
        origin.setMoveDelay("P1D");
        origin.setCapacity(capacity);
        newContainer.setOriginLocation(origin);

        Location archive = new Location();
        archive.setBlobStoreId(0);
        archive.setContainerName("archive");
        newContainer.setArchiveLocation(archive);

        VirtualContainerResource resource = new VirtualContainerResource(app);
        assertThatThrownBy(() -> resource.updateContainer(0, newContainer))
                .isInstanceOf(WebApplicationException.class)
                .hasCauseExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testCapacityAndEvictNever() throws Exception {
        app.getBlobStore(0).createContainerInLocation(null, "archive");

        // Setting capacity and eviction to "never" (negative) is valid
        Long capacity = 50000L;
        VirtualContainer newContainer = new VirtualContainer();
        newContainer.setName("vBucket");
        Location origin = new Location();
        origin.setBlobStoreId(0);
        origin.setContainerName("container");
        origin.setMoveDelay("-P1D");
        origin.setCapacity(capacity);
        newContainer.setOriginLocation(origin);

        Location archive = new Location();
        archive.setBlobStoreId(0);
        archive.setContainerName("archive");
        newContainer.setArchiveLocation(archive);

        VirtualContainerResource resource = new VirtualContainerResource(app);
        resource.updateContainer(0, newContainer);

        String containerPrefix = Joiner.on(".").join(VirtualContainerResource.VIRTUAL_CONTAINER_PREFIX, "0");
        String primaryLocationPrefix = Joiner.on(".").join(containerPrefix, VirtualContainer.PRIMARY_TIER_PREFIX);
        Configuration config = app.getConfiguration();
        assertThat(config.getProperty(Joiner.on(".").join(primaryLocationPrefix, Location.BLOB_STORE_ID_FIELD)))
                .isEqualTo("0");
        assertThat(config.getProperty(Joiner.on(".").join(primaryLocationPrefix, Location.CONTAINER_NAME_FIELD)))
                .isEqualTo("container");
        assertThat(config.getProperty(Joiner.on(".").join(primaryLocationPrefix, StoragePolicy.CAPACITY_SETTING)))
                .isEqualTo(capacity.toString());
        assertThat(config.getProperty(Joiner.on(".").join(primaryLocationPrefix, WriteBackPolicy.EVICT_DELAY)))
                .isEqualTo("-P1D");
        assertThat(config.getProperty(Joiner.on(".").join(primaryLocationPrefix, "policy")))
                .isEqualTo(LRUStoragePolicy.class.getSimpleName());
    }

    @Test
    public void testUpdateContainerConfig() throws Exception {
        String containerName = UtilsTest.createRandomBlobName();

        VirtualContainerResource resource = new VirtualContainerResource(app);

        VirtualContainer newContainer = new VirtualContainer();
        newContainer.setName(containerName);
        Location origin = new Location();
        origin.setBlobStoreId(0);
        origin.setContainerName("container");
        origin.setMoveDelay("-P1D");
        origin.setCopyDelay("-P1D");
        origin.setCapacity(1);
        newContainer.setOriginLocation(origin);

        Location archive = new Location();
        archive.setBlobStoreId(0);
        archive.setContainerName("archive");
        newContainer.setArchiveLocation(archive);

        resource.createContainer(newContainer);
        assertThat(newContainer.getId()).isNotEqualTo(0);

        StoragePolicy policy = (StoragePolicy) app.getBlobStore(containerName);
        assertThat(policy.getCapacity()).isEqualTo(1);

        origin = newContainer.getOriginLocation();
        origin.setCapacity(2);
        resource.updateContainer(newContainer.getId(), newContainer);
        assertThat(policy.getCapacity()).isEqualTo(2);
    }

    private Properties getDefaultProperties() {
        Properties properties = new Properties();
        properties.setProperty("bounce.backends", "0");
        String backendPrefix = Joiner.on(".").join(BounceBlobStore.STORE_PROPERTY, "0");
        properties.setProperty(Joiner.on(".").join(backendPrefix, "jclouds.provider"), "transient");
        properties.setProperty(Joiner.on(".").join(backendPrefix, "nickname"), "memory");
        properties.setProperty("s3proxy.endpoint", "http://127.0.0.1:0");
        properties.setProperty("s3proxy.authorization", "aws-v2");
        properties.setProperty("jclouds.provider", "bounce");
        return properties;
    }

    private void setDefaultContainerProperties() {
        app.getBlobStore(0).createContainerInLocation(null, "container");
        app.getBlobStore(0).createContainerInLocation(null, "cache");
        app.getBlobStore(0).createContainerInLocation(null, "archive");
        Properties properties = new Properties();
        String virtualContainerPrefix = VirtualContainerResource.VIRTUAL_CONTAINER_PREFIX + "." + 0;
        properties.setProperty(Joiner.on(".").join(virtualContainerPrefix, VirtualContainer.NAME), "vBucket");
        String primaryTierPrefix = Joiner.on(".").join(virtualContainerPrefix, VirtualContainer.PRIMARY_TIER_PREFIX);
        properties.setProperty(Joiner.on(".").join(primaryTierPrefix, Location.BLOB_STORE_ID_FIELD), "0");
        properties.setProperty(Joiner.on(".").join(primaryTierPrefix, Location.CONTAINER_NAME_FIELD), "container");
        String cacheTierPrefix = Joiner.on(".").join(virtualContainerPrefix, VirtualContainer.CACHE_TIER_PREFIX);
        properties.setProperty(Joiner.on(".").join(cacheTierPrefix, Location.BLOB_STORE_ID_FIELD), "0");
        properties.setProperty(Joiner.on(".").join(cacheTierPrefix, Location.CONTAINER_NAME_FIELD), "cache");
        properties.setProperty(Joiner.on(".").join(cacheTierPrefix, "policy"), "WriteBackPolicy");
        properties.setProperty(Joiner.on(".").join(cacheTierPrefix, WriteBackPolicy.EVICT_DELAY), "P0D");
        properties.setProperty(Joiner.on(".").join(cacheTierPrefix, WriteBackPolicy.COPY_DELAY), "P0D");
        properties.setProperty("bounce.containers", "0");

        app.getConfiguration().setAll(properties);
    }
}
