/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.google.auto.service.AutoService;

import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageMetadata;

@AutoService(BouncePolicy.class)
public final class MoveEverythingPolicy extends MovePolicy {
    @Override
    public String toString() {
        return "everything";
    }

    @Override
    public BounceResult reconcileObject(String container, BounceStorageMetadata sourceObject, StorageMetadata
            destinationObject) {
        if ((sourceObject == null) && (destinationObject == null)) {
            throw new AssertionError("At least one of source or destination objects must be non-null");
        }

        if (sourceObject == null) {
            return maybeRemoveDestinationObject(container, destinationObject);
        } else if (sourceObject.getRegions().equals(BounceBlobStore.FAR_ONLY)) {
            return BounceResult.NO_OP;
        }

        // There two cases lefts: NEAR_ONLY and EVERYWHERE. Check which one it is before we just create a link
        BlobMetadata sourceMeta = getSource().blobMetadata(container, sourceObject.getName());
        BlobMetadata destinationMeta = getDestination().blobMetadata(container, sourceObject.getName());
        if ((destinationMeta != null) && sourceMeta.getETag().equalsIgnoreCase(destinationMeta.getETag())) {
            return Utils.createBounceLink(this, sourceMeta);
        } else {
            return Utils.copyBlobAndCreateBounceLink(this, container, sourceMeta.getName());
        }
    }
}
