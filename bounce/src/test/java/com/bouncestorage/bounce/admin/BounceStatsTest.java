/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static org.assertj.core.api.Assertions.assertThat;

import javax.ws.rs.HttpMethod;

import org.influxdb.dto.Serie;
import org.junit.Test;

public class BounceStatsTest {
    private BounceStats stats = new BounceStats();

    @Test
    public void testNoStats() {
        assertThat(stats.prepareSerie()).isNull();
    }

    @Test
    public void testRemoveProcessedValues() {
        stats.logOperation(HttpMethod.GET, "provider", "container", "foo", Long.valueOf(1), Long.valueOf(0));
        stats.logOperation(HttpMethod.PUT, "provider", "container", "foo", Long.valueOf(2), Long.valueOf(0));
        stats.logOperation(HttpMethod.PUT, "provider", "container", "foo", Long.valueOf(3), Long.valueOf(0));
        Serie serie = stats.prepareSerie();
        stats.logOperation(HttpMethod.GET, "provider", "container", "foo", Long.valueOf(4), Long.valueOf(0));
        stats.logOperation(HttpMethod.GET, "provider", "container", "foo", Long.valueOf(5), Long.valueOf(0));
        stats.removeProcessedValues(serie);
        assertThat(stats.getOpsQueue()).hasSize(2);
        assertThat(stats.getOpsQueue().peek()[5]).isEqualTo(Long.valueOf(4));
    }
}
