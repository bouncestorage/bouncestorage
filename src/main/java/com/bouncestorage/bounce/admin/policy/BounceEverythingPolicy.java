/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import com.bouncestorage.bounce.admin.BouncePolicy;

import org.jclouds.blobstore.domain.StorageMetadata;

public class BounceEverythingPolicy implements BouncePolicy {
    @Override
    public final String toString() {
        return "everything";
    }

    @Override
    public final boolean test(StorageMetadata metadata) {
        return true;
    }
}
