/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.admin.BouncePolicy;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.logging.Logger;

public final class BounceNothingPolicy implements BouncePolicy {
    private BlobStore source;
    private BlobStore destination;
    private Logger logger = Logger.NULL;

    public void setBlobStores(BlobStore sourceStore, BlobStore destinationStore) {
        this.source = sourceStore;
        this.destination = destinationStore;
    }

    @Override
    public Blob getBlob(String container, String blobName, GetOptions options) {
        return source.getBlob(container, blobName, options);
    }

    @Override
    public String putBlob(String container, Blob blob, PutOptions options) {
        return source.putBlob(container, blob, options);
    }

    @Override
    public BounceResult reconcileObject(String container, BounceStorageMetadata sourceObject, StorageMetadata destinationObject) {
        return BounceResult.NO_OP;
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
    public Logger getLogger() {
        return logger;
    }
}
