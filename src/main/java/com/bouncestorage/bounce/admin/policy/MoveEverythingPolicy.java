/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.google.auto.service.AutoService;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.logging.Logger;

@AutoService(BouncePolicy.class)
public final class MoveEverythingPolicy extends MovePolicy {
    private BlobStore near;
    private BlobStore far;
    private Logger logger = Logger.NULL;

    public void setBlobStores(BlobStore nearStore, BlobStore farStore) {
        this.near = nearStore;
        this.far = farStore;
    }

    public BlobStore getSource() {
        return near;
    }

    public BlobStore getDestination() {
        return far;
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public String toString() {
        return "everything";
    }

    @Override
    public BounceResult reconcileObject(String container, BounceStorageMetadata sourceObject, StorageMetadata
            destinationObject) {
        if (sourceObject == null) {
            return Utils.maybeRemoveObject(getDestination(), container,
                    destinationObject);
        }

        BlobMetadata sourceMeta = getSource().blobMetadata(container, sourceObject.getName());
        if (BounceLink.isLink(sourceMeta)) {
            return BounceResult.NO_OP;
        }

        if (destinationObject ==  null) {
            return Utils.copyBlobAndCreateBounceLink(this, container, sourceMeta.getName());
        }

        BlobMetadata destinationMeta = getDestination().blobMetadata(container, destinationObject.getName());
        if (destinationMeta.getETag().equalsIgnoreCase(sourceMeta.getETag())) {
            return Utils.createBounceLink(this, sourceMeta);
        }

        return Utils.copyBlobAndCreateBounceLink(this, container, sourceMeta.getName());
    }
}
