/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static com.google.common.base.Throwables.propagate;

import java.io.IOException;

import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.google.auto.service.AutoService;

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
        }

        try {
            return maybeMoveObject(container, sourceObject, destinationObject);
        } catch (IOException e) {
            throw propagate(e);
        }
    }

}
