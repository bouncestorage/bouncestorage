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
import org.jclouds.blobstore.domain.StorageMetadata;

@AutoService(BouncePolicy.class)
public final class LastModifiedTimePolicy extends WriteBackPolicy {
    @Override
    public void init(BounceApplication app, Configuration config) {
        config.setProperty(COPY_DELAY, Duration.ofSeconds(-1).toString());
        super.init(app, config);
    }

    public boolean isObjectExpired(StorageMetadata metadata) {
        return super.isObjectExpired(metadata, evictDelay);
    }
}
