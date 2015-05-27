/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import javax.ws.rs.HttpMethod;

import org.influxdb.dto.Serie;
import org.junit.Test;

public class BounceStatsTest {
    private BounceStats stats = new BounceStats();

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
}
