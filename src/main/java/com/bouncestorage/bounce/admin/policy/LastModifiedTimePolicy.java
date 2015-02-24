/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static com.google.common.base.Preconditions.checkNotNull;

import java.time.Duration;
import java.time.Instant;

import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.bouncestorage.bounce.admin.BounceService;
import com.google.auto.service.AutoService;

import org.apache.commons.configuration.Configuration;

@AutoService(BouncePolicy.class)
public final class LastModifiedTimePolicy extends MovePolicy {
    public static final String DURATION = "duration";
    private BounceService service;
    private Duration timeAgo;

    public void init(BounceService inService, Configuration config) {
        this.service = checkNotNull(inService);
        this.timeAgo = checkNotNull(Duration.parse(config.getString(DURATION)));
    }

    @Override
    public boolean test(Utils.ListBlobMetadata blobMetadata) {
        Instant now = service.getClock().instant();
        Instant then = blobMetadata.metadata().getLastModified().toInstant();
        return now.minus(timeAgo).isAfter(then);
    }
}
