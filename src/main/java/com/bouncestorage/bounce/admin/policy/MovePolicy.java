/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import java.io.IOException;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.google.common.collect.ImmutableSet;

public abstract class MovePolicy implements BouncePolicy {
    public static BounceResult moveBounce(BounceBlobStore bounceBlobStore, String container, BounceStorageMetadata meta)
            throws IOException {
        ImmutableSet<BounceBlobStore.Region> regions = meta.getRegions();
        if (regions == BounceBlobStore.EVERYWHERE) {
            bounceBlobStore.createBounceLink(bounceBlobStore.blobMetadata(container, meta.getName()));
            return BounceResult.NO_OP;
        } else {
            if (bounceBlobStore.copyBlobAndCreateBounceLink(container, meta.getName()) != null) {
                return BounceResult.MOVE;
            } else {
                return BounceResult.NO_OP;
            }
        }
    }

    @Override
    public final BounceResult bounce(BounceBlobStore bounceBlobStore, String container, BounceStorageMetadata meta)
            throws IOException {
        return moveBounce(bounceBlobStore, container, meta);
    }
}
