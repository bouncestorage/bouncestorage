/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import com.bouncestorage.bounce.admin.BouncePolicy;

import com.google.auto.service.AutoService;
import org.jclouds.blobstore.domain.StorageMetadata;

@AutoService(BouncePolicy.class)
public final class BounceEverythingPolicy implements BouncePolicy {
    @Override
    public String toString() {
        return "everything";
    }

    @Override
    public boolean test(StorageMetadata metadata) {
        return true;
    }
}
