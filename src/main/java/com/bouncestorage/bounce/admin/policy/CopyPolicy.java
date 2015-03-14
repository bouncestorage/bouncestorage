/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import java.io.IOException;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.BounceStorageMetadata;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.GetOptions;

public final class CopyPolicy extends MarkerPolicy {
    @Override
    public boolean test(StorageMetadata metadata) {
        return true;
    }

    @Override
    public Blob getBlob(String container, String blobName, GetOptions options) {
        return getSource().getBlob(container, blobName, options);
    }

    public static BounceResult copyBounce(BounceBlobStore blobStore, String container, BounceStorageMetadata meta) throws IOException {
        if (meta.getRegions().contains(BounceBlobStore.Region.FAR) || !meta.hasMarkerBlob()) {
            return BounceResult.NO_OP;
        }

        Blob b = blobStore.copyBlob(container, meta.getName());
        if (b == null) {
            return BounceResult.NO_OP;
        } else {
            blobStore.removeBlob(container, meta.getName() + MarkerPolicy.LOG_MARKER_SUFFIX);
        }
        return BounceResult.COPY;
    }

    @Override
    public BounceResult bounce(BounceBlobStore blobStore, String container, BounceStorageMetadata meta) throws
            IOException {
        return copyBounce(blobStore, container, meta);
    }
}
