/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static com.bouncestorage.bounce.UtilsTest.assertStatus;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.HttpMethod;

import com.bouncestorage.bounce.UtilsTest;
import com.bouncestorage.bounce.admin.policy.MigrationPolicy;
import com.bouncestorage.bounce.admin.policy.WriteBackPolicy;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;

import org.influxdb.dto.Serie;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.junit.After;
import org.junit.Test;

public class BounceStatsTest {
    private BounceApplication app;
    private BounceStats stats = new BounceStats();
    private Blob blob;
    private String container;

    @After
    public void tearDown() throws Exception {
        if (app != null) {
            app.stop();
        }
    }

    @Test
    public void testNoStats() {
        assertThat(stats.prepareSeries()).isEmpty();
    }

    @Test
    public void testRemoveProcessedValues() {
        stats.logOperation(HttpMethod.GET, 0, "container", "foo", Long.valueOf(1), Long.valueOf(0));
        stats.logOperation(HttpMethod.PUT, 0, "container", "foo", Long.valueOf(2), Long.valueOf(0));
        stats.logOperation(HttpMethod.PUT, 0, "container", "foo", Long.valueOf(3), Long.valueOf(0));
        List<Serie> series = stats.prepareSeries();
        stats.logOperation(HttpMethod.GET, 0, "container", "foo", Long.valueOf(4), Long.valueOf(0));
        stats.logOperation(HttpMethod.GET, 0, "container", "foo", Long.valueOf(5), Long.valueOf(0));
        for (Serie serie : series) {
            stats.removeProcessedValues(serie);
        }
        assertThat(stats.getQueue()).hasSize(2);
        assertThat(stats.getQueue().peek().getValues().get(2)).isEqualTo(Long.valueOf(4));
    }

    @Test
    public void testLogOperation() throws Exception {
        stats.logOperation(HttpMethod.GET, 0, "container", "foo", Long.valueOf(1), Long.valueOf(1));
        StatsQueueEntry entry = stats.getQueue().peek();
        assertThat(entry.getDbSeries().getName()).startsWith(BounceStats.DBSeries.OPS_SERIES);
    }

    @Test
    public void testLogObjectStoreStats() throws Exception {
        stats.logObjectStoreStats(0, "bucket", 1337, 42);
        StatsQueueEntry entry = stats.getQueue().peek();
        assertThat(entry.getDbSeries().getName()).startsWith(BounceStats.DBSeries.OBJECT_STORE_SERIES);
    }

    @Test
    public void testDifferentSeries() throws Exception {
        stats.logObjectStoreStats(0, "bucket", 1337, 42);
        stats.logOperation(HttpMethod.PUT, 0, "bucket", "foo", Long.valueOf(1), Long.valueOf(2));
        List<Serie> series = stats.prepareSeries();
        Serie first = series.get(0);
        assertThat(first.getName()).isEqualTo(
                BounceStats.DBSeries.OBJECT_STORE_SERIES + ".provider.0.container.bucket");
        assertThat(first.getColumns()).isEqualTo(BounceStats.DBSeries.OBJECT_STORE_COLUMNS.toArray());
        assertThat(first.getRows().get(0)).containsKey("objects");
        assertThat(first.getRows().get(0).get("objects")).isEqualTo(42L);
        assertThat(first.getRows().get(0)).containsKey("size");
        assertThat(first.getRows().get(0).get("size")).isEqualTo(1337L);
        Serie second = series.get(1);
        assertThat(second.getName()).isEqualTo(BounceStats.DBSeries.OPS_SERIES + ".provider.0.container.bucket.op.PUT");
        assertThat(second.getColumns()).isEqualTo(BounceStats.DBSeries.OPS_COLUMNS.toArray());
        assertThat(second.getRows().get(0)).containsKey("duration");
        assertThat(second.getRows().get(0)).containsKey("size");
        assertThat(second.getRows().get(0).get("size")).isEqualTo(1L);
    }

    @Test
    public void testLogContainerStatsAfterMove() throws Exception {
        setupApplication();
        setupContainerAndUploadBlob();
        UtilsTest.switchPolicyforContainer(app, container, WriteBackPolicy.class, ImmutableMap.of(
                WriteBackPolicy.COPY_DELAY, "P0D", WriteBackPolicy.EVICT_DELAY, "P0D"));
        app.getBlobStore(container).createContainerInLocation(null, container);
        BounceService.BounceTaskStatus status = runBounce();
        assertStatus(status, status::getMovedObjectCount).isEqualTo(1);
        List<StatsQueueEntry> loggedStats = getObjectStoreStats();
        boolean sourceStats = false;
        boolean destinationStats = false;
        for (StatsQueueEntry entry : loggedStats) {
            ArrayList<Object> values = entry.getValues();
            assertThat(values.get(2)).isEqualTo(1L);
            String name = entry.getDbSeries().getName();
            if (name.startsWith(BounceStats.DBSeries.OBJECT_STORE_SERIES + ".provider.0.")) {
                assertThat(values.get(1)).isEqualTo(0L);
                sourceStats = true;
            } else if (name.startsWith(BounceStats.DBSeries.OBJECT_STORE_SERIES + ".provider.1.")) {
                assertThat(values.get(1)).isEqualTo(blob.getMetadata().getContentMetadata().getContentLength());
                destinationStats = true;
            }
        }
        assertThat(sourceStats).isTrue();
        assertThat(destinationStats).isTrue();
    }

    @Test
    public void testLogContainerStatsAfterCopy() throws Exception {
        setupApplication();
        setupContainerAndUploadBlob();
        UtilsTest.switchPolicyforContainer(app, container, WriteBackPolicy.class, ImmutableMap.of(
                WriteBackPolicy.COPY_DELAY, "P0D", WriteBackPolicy.EVICT_DELAY, "P1D"));
        app.getBlobStore(container).createContainerInLocation(null, container);
        BounceService.BounceTaskStatus status = runBounce();
        assertStatus(status, status::getCopiedObjectCount).isEqualTo(1);
        List<StatsQueueEntry> loggedStats = getObjectStoreStats();
        boolean sourceStats = false;
        boolean destinationStats = false;
        for (StatsQueueEntry entry : loggedStats) {
            ArrayList<Object> values = entry.getValues();
            assertThat(values.get(2)).isEqualTo(1L);
            assertThat(values.get(1)).isEqualTo(blob.getMetadata().getContentMetadata().getContentLength());
            String name = entry.getDbSeries().getName();
            if (name.startsWith(BounceStats.DBSeries.OBJECT_STORE_SERIES + ".provider.0.")) {
                sourceStats = true;
            } else if (name.startsWith(BounceStats.DBSeries.OBJECT_STORE_SERIES + ".provider.1.")) {
                destinationStats = true;
            }
        }
        assertThat(sourceStats).isTrue();
        assertThat(destinationStats).isTrue();
    }

    @Test
    public void testLogContainerStatsAfterMigrate() throws Exception {
        setupApplication();
        setupContainerAndUploadBlob();
        UtilsTest.switchPolicyforContainer(app, container, MigrationPolicy.class);
        app.getBlobStore(container).createContainerInLocation(null, container);
        BounceService.BounceTaskStatus status = runBounce();
        assertStatus(status, status::getMovedObjectCount).isEqualTo(1);
        List<StatsQueueEntry> loggedStats = getObjectStoreStats();
        boolean sourceStats = false;
        boolean destinationStats = false;
        for (StatsQueueEntry entry : loggedStats) {
            ArrayList<Object> values = entry.getValues();
            String name = entry.getDbSeries().getName();
            if (name.startsWith(BounceStats.DBSeries.OBJECT_STORE_SERIES + ".provider.0.")) {
                assertThat(values.get(2)).isEqualTo(1L);
                assertThat(values.get(1)).isEqualTo(0L);
                sourceStats = true;
            } else if (name.startsWith(BounceStats.DBSeries.OBJECT_STORE_SERIES + ".provider.1.")) {
                assertThat(values.get(2)).isEqualTo(1L);
                assertThat(values.get(1)).isEqualTo(blob.getMetadata().getContentMetadata().getContentLength());
                destinationStats = true;
            }
        }
        assertThat(sourceStats).isTrue();
        assertThat(destinationStats).isTrue();
    }

    private void setupApplication() {
        synchronized (BounceApplication.class) {
            app = new BounceApplication();
        }
        app.useRandomPorts();
        app.registerConfigurationListener();
        UtilsTest.createTestProvidersConfig(app.getConfiguration());
    }

    private void setupContainerAndUploadBlob() {
        BlobStore blobStore = app.getBlobStore(0);
        container = UtilsTest.createRandomContainerName();
        String blobName = UtilsTest.createRandomBlobName();
        byte[] blobContent = "foo".getBytes();
        blob = UtilsTest.makeBlob(blobStore, blobName, ByteSource.wrap(blobContent));
        blobStore.createContainerInLocation(null, container);
        blobStore.putBlob(container, blob);
    }

    private BounceService.BounceTaskStatus runBounce() throws Exception {
        BounceService service = new BounceService(app);
        BounceService.BounceTaskStatus status = service.bounce(container);
        status.future().get();
        return status;
    }

    private List<StatsQueueEntry> getObjectStoreStats() {
        BounceStats appStats = app.getBounceStats();
        return appStats.getQueue().stream()
                .filter(entry -> entry.getDbSeries().getName().startsWith(BounceStats.DBSeries.OBJECT_STORE_SERIES))
                .collect(Collectors.toList());
    }
}
