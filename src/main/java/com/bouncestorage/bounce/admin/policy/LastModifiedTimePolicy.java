/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import com.bouncestorage.bounce.admin.BouncePolicy;

import com.bouncestorage.bounce.admin.BounceService;
import com.google.auto.service.AutoService;
import org.jclouds.blobstore.domain.StorageMetadata;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

@AutoService(BouncePolicy.class)
public final class LastModifiedTimePolicy implements BouncePolicy {
    public static final String DURATION = LastModifiedTimePolicy.class.getCanonicalName() + ".DURATION";
    private BounceService service;
    private Duration timeAgo;

    public void init(BounceService inService, Properties properties) {
        this.service = checkNotNull(inService);
        this.timeAgo = checkNotNull(Duration.parse(properties.getProperty(DURATION)));
    }

    @Override
    public boolean test(StorageMetadata metadata) {
        Instant now = service.getClock().instant();
        Instant then = metadata.getLastModified().toInstant();
        return now.minus(timeAgo).isAfter(then);
    }
}
