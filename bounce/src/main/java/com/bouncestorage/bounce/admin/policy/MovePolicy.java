/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static com.google.common.base.Throwables.propagate;

import java.io.IOException;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.Utils;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.GetOptions;

public abstract class MovePolicy extends MarkerPolicy {
    @Override
    public final Blob getBlob(String container, String blobName, GetOptions options) {
        Blob blob = super.getBlob(container, blobName, options);
        if (blob == null) {
            return null;
        }
        BlobMetadata meta = blob.getMetadata();
        if (BounceLink.isLink(meta)) {
            try {
                Utils.copyBlob(getDestination(), getSource(), container, container, blobName);
            } catch (IOException e) {
                return blob;
            }
        }
        return getSource().getBlob(container, blobName, options);
    }

    @Override
    public BlobMetadata blobMetadata(String container, String blobName) {
        BlobMetadata meta = getSource().blobMetadata(container, blobName);
        if (meta != null) {
            if (BounceLink.isLink(meta)) {
                Blob linkBlob = getSource().getBlob(container, blobName);
                if (linkBlob != null) {
                    try {
                        return BounceLink.fromBlob(linkBlob).getBlobMetadata();
                    } catch (IOException e) {
                        throw propagate(e);
                    }
                } else {
                    return null;
                }
            }
        }
        return meta;
    }

    protected final BounceResult maybeMoveObject(String container, BounceStorageMetadata sourceObject,
            StorageMetadata destinationObject) throws IOException {
        if (sourceObject.getRegions().equals(BounceBlobStore.FAR_ONLY)) {
            return BounceResult.NO_OP;
        }
        if (sourceObject.getRegions().equals(BounceBlobStore.EVERYWHERE)) {
            BlobMetadata sourceMetadata = getSource().blobMetadata(container, sourceObject.getName());
            BlobMetadata destinationMetadata = getDestination().blobMetadata(container, destinationObject.getName());
            if (destinationMetadata.getETag().equalsIgnoreCase(sourceMetadata.getETag())) {
                Utils.createBounceLink(this, sourceMetadata);
                return BounceResult.LINK;
            }
        }

        Utils.copyBlobAndCreateBounceLink(getSource(), getDestination(), container,
                sourceObject.getName());
        return BounceResult.MOVE;
    }
}
