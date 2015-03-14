/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.BounceStorageMetadata;

import org.jclouds.blobstore.domain.StorageMetadata;

public final class BounceNothingPolicy extends NoBouncePolicy {
    @Override
    public boolean test(StorageMetadata metadata) {
        return false;
    }

    @Override
    public BounceResult bounce(BounceBlobStore store, String container, BounceStorageMetadata blob) {
        return BounceResult.NO_OP;
    }
}
