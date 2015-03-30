/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import java.time.Duration;

import com.bouncestorage.bounce.admin.BouncePolicy;
import com.bouncestorage.bounce.admin.BounceService;
import com.google.auto.service.AutoService;

import org.apache.commons.configuration.Configuration;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.options.GetOptions;

@AutoService(BouncePolicy.class)
public final class CopyPolicy extends WriteBackPolicy {
    @Override
    public void init(BounceService inService, Configuration config) {
        config.setProperty(EVICT_DELAY, Duration.ofSeconds(-1).toString());
        config.setProperty(COPY_DELAY, Duration.ofSeconds(0).toString());
        super.init(inService, config);
    }

    @Override
    public Blob getBlob(String container, String blobName, GetOptions options) {
        return getSource().getBlob(container, blobName, options);
    }
}
