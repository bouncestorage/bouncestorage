/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import java.io.IOException;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.BounceStorageMetadata;
import com.google.common.collect.ImmutableSet;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.options.GetOptions;

public abstract class MovePolicy extends MarkerPolicy {
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
    public final Blob getBlob(String container, String blobName, GetOptions options) {
        BlobMetadata meta = getSource().blobMetadata(container, blobName);
        if (BounceLink.isLink(meta)) {
            Blob blob = getDestination().getBlob(container, blobName, options);
            try {
                Utils.copyBlob(getDestination(), getSource(), container, container, blobName);
            } catch (IOException e) {
                return blob;
            }
        }
        return getSource().getBlob(container, blobName, options);
    }
}
