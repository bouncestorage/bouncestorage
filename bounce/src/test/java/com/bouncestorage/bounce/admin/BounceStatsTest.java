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
        stats.logOperation(HttpMethod.GET, "container", "foo", Long.valueOf(1));
        stats.logOperation(HttpMethod.PUT, "container", "foo", Long.valueOf(2));
        stats.logOperation(HttpMethod.PUT, "container", "foo", Long.valueOf(3));
        Serie serie = stats.prepareSerie();
        stats.logOperation(HttpMethod.GET, "container", "foo", Long.valueOf(4));
        stats.logOperation(HttpMethod.GET, "container", "foo", Long.valueOf(5));
        stats.removeProcessedValues(serie);
        assertThat(stats.getOpsQueue()).hasSize(2);
        assertThat(stats.getOpsQueue().peek()[4]).isEqualTo(Long.valueOf(4));
    }
}
