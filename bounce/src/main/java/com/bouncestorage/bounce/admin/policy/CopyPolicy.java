/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static com.google.common.base.Throwables.propagate;

import java.io.IOException;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.google.auto.service.AutoService;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.GetOptions;

@AutoService(BouncePolicy.class)
public final class CopyPolicy extends MarkerPolicy {
    @Override
    public Blob getBlob(String container, String blobName, GetOptions options) {
        return getSource().getBlob(container, blobName, options);
    }

    @Override
    public BounceResult reconcileObject(String container, BounceStorageMetadata sourceObject, StorageMetadata
            destinationObject) {
        if ((sourceObject == null) && (destinationObject == null)) {
            throw new AssertionError("At least one of source or destination objects must be non-null");
        }

        if (sourceObject == null) {
            logger.debug("Removing {}: {}", destinationObject.getName(), destinationObject);
            return maybeRemoveDestinationObject(container, destinationObject);
        }

        // maybeCopyObject handles the cases of EVERYWHERE, FAR_ONLY, and NEAR_ONLY
        try {
            return maybeCopyObject(container, sourceObject, destinationObject);
        } catch (IOException e) {
            throw propagate(e);
        }
    }

}
