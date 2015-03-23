/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.time.Instant;

import com.bouncestorage.bounce.admin.BouncePolicy;
import com.bouncestorage.bounce.admin.BounceService;
import com.google.auto.service.AutoService;

import org.apache.commons.configuration.Configuration;
import org.jclouds.blobstore.domain.StorageMetadata;

@AutoService(BouncePolicy.class)
public final class LastModifiedTimePolicy extends MovePolicy {
    public static final String DURATION = "duration";
    private BounceService service;
    private Duration timeAgo;

    public void init(BounceService inService, Configuration config) {
        config.getKeys().forEachRemaining(s -> logger.info("policy: {} = {}", s, config.getString(s)));
        this.service = requireNonNull(inService);
        this.timeAgo = requireNonNull(Duration.parse(config.getString(DURATION)));
    }

    @Override
    public boolean test(StorageMetadata metadata) {
        Instant now = service.getClock().instant();
        Instant then = metadata.getLastModified().toInstant();
        return now.minus(timeAgo).isAfter(then);
    }
}
