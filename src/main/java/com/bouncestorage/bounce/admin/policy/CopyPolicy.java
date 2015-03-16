/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static com.google.common.base.Throwables.propagate;

import java.io.IOException;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.google.auto.service.AutoService;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.GetOptions;

@AutoService(BouncePolicy.class)
public final class CopyPolicy extends MarkerPolicy {
    public static BounceResult copyBounce(BounceBlobStore blobStore, String container, BounceStorageMetadata meta)
            throws IOException {
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
        } else if (sourceObject.getRegions().equals(BounceBlobStore.FAR_ONLY)) {
            return BounceResult.NO_OP;
        } else if (sourceObject.getRegions().equals(BounceBlobStore.EVERYWHERE)) {
            return BounceResult.NO_OP;
        }

        // At this point, it must be NEAR_ONLY
        return maybeCopyObject(container, sourceObject);
    }

    private BounceResult copyObject(BounceStorageMetadata object, String container, BlobStore sourceStore,
                                    BlobStore destinationStore) {
        try {
            Blob blob = Utils.copyBlob(sourceStore, destinationStore, container, container, object.getName());
            if (blob == null) {
                return BounceResult.NO_OP;
            }
            getSource().removeBlob(container, object.getName() + MarkerPolicy.LOG_MARKER_SUFFIX);
            return BounceResult.COPY;
        } catch (IOException e) {
            throw propagate(e);
        }
    }

    private BounceResult maybeCopyObject(String container, BounceStorageMetadata sourceObject) {
        BlobMetadata destinationMeta = getDestination().blobMetadata(container, sourceObject.getName());
        BlobMetadata sourceMeta = getSource().blobMetadata(container, sourceObject.getName());
        if ((destinationMeta != null) && destinationMeta.getETag().equalsIgnoreCase(sourceMeta.getETag())) {
            return BounceResult.NO_OP;
        }

        return copyObject(sourceObject, container, getSource(), getDestination());
    }
}
