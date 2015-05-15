/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import java.util.ArrayList;

import com.google.auto.value.AutoValue;

@AutoValue
public abstract class StatsQueueEntry {
    public static StatsQueueEntry create(BounceStats.DBSeries dbSeries, ArrayList<Object> values) {
        return new AutoValue_StatsQueueEntry(dbSeries, values);
    }

    public abstract BounceStats.DBSeries getDbSeries();
    public abstract ArrayList<Object> getValues();
}
