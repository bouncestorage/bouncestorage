/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static com.google.common.base.Throwables.propagate;

import java.io.IOException;
import java.time.Duration;

import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.bouncestorage.bounce.admin.BounceService;
import com.google.auto.service.AutoService;

import org.apache.commons.configuration.Configuration;
import org.jclouds.blobstore.domain.StorageMetadata;

@AutoService(BouncePolicy.class)
public final class MoveEverythingPolicy extends WriteBackPolicy {
    @Override
    public String toString() {
        return "everything";
    }

    @Override
    public void init(BounceService inService, Configuration config) {
        config.setProperty(EVICT_DELAY, Duration.ofSeconds(0).toString());
        config.setProperty(COPY_DELAY, Duration.ofSeconds(-1).toString());
        super.init(inService, config);
    }
}
