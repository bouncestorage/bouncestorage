/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import java.time.Duration;

import com.bouncestorage.bounce.admin.BounceApplication;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.google.auto.service.AutoService;

import org.apache.commons.configuration.Configuration;

@AutoService(BouncePolicy.class)
public final class MoveEverythingPolicy extends WriteBackPolicy {
    @Override
    public String toString() {
        return "everything";
    }

    @Override
    public void init(BounceApplication app, Configuration config) {
        config.setProperty(EVICT_DELAY, Duration.ofSeconds(0).toString());
        config.setProperty(COPY_DELAY, Duration.ofSeconds(-1).toString());
        super.init(app, config);
    }
}
