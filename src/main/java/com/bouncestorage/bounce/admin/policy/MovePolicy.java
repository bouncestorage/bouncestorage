/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import java.io.IOException;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.admin.BouncePolicy;

import org.jclouds.blobstore.domain.BlobMetadata;

public abstract class MovePolicy implements BouncePolicy {
    @Override
    public final BounceResult bounce(BlobMetadata meta, BounceBlobStore bounceBlobStore) throws IOException {
        if (meta.getUserMetadata().containsKey(CopyPolicy.COPIED_METADATA_KEY)) {
            return BounceResult.NO_OP;
        }
        bounceBlobStore.copyBlobAndCreateBounceLink(meta.getContainer(), meta.getName());
        return BounceResult.MOVE;
    }
}
