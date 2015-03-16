/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Throwables.propagate;

import java.io.IOException;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.BounceStorageMetadata;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.logging.Logger;

public final class CopyPolicy extends MarkerPolicy {
    private BlobStore source;
    private BlobStore destination;
    private Logger logger = Logger.NULL;

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

    public void setBlobStores(BlobStore sourceStore, BlobStore destinationStore) {
        this.source = sourceStore;
        this.destination = destinationStore;
    }

    @Override
    public BlobStore getSource() {
        return source;
    }

    @Override
    public BlobStore getDestination() {
        return destination;
    }

    @Override
    public Blob getBlob(String container, String blobName, GetOptions options) {
        return source.getBlob(container, blobName, options);
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public BounceResult reconcileObject(String container, BounceStorageMetadata sourceObject, StorageMetadata
            destinationObject) {
        String blobName;
        if (sourceObject != null) {
            blobName = sourceObject.getName();
        } else {
            blobName = destinationObject.getName();
        }

        if (source != null) {
            getLogger().debug("Reconciling %s in %s (source: %s, destination: %s)", blobName, sourceObject.getRegions(),
                    sourceObject, destinationObject);
            return maybeCopyObject(sourceObject, destinationObject, container);
        }

        if (source == null) {
            getLogger().debug("Removing %s: %s", blobName, destinationObject);
            return maybeRemoveDestinationObject(container, destinationObject);
        }

        return BounceResult.NO_OP;
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
            propagate(e);
        }

        // Should never reach here
        return BounceResult.NO_OP;
    }

    private BounceResult maybeCopyObject(BounceStorageMetadata sourceObject, StorageMetadata destinationObject,
                                         String container) {
        sourceObject = requireNonNull(sourceObject);
        if (destinationObject == null) {
            return copyObject(sourceObject, container, getSource(), getDestination());
        }

        BlobMetadata sourceMeta = getSource().blobMetadata(container, sourceObject.getName());
        if (BounceLink.isLink(sourceMeta)) {
            return BounceResult.NO_OP;
        }
        BlobMetadata destinationMeta = getDestination().blobMetadata(container, destinationObject.getName());
        if (destinationMeta.getETag().equalsIgnoreCase(sourceMeta.getETag())) {
            // The objects are equivalent -- nothing to do
            return BounceResult.NO_OP;
        }
        return copyObject(sourceObject, container, getSource(), getDestination());
    }
}
