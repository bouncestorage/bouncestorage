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
import org.jclouds.logging.Logger;

public final class ObserverPolicy implements BouncePolicy {
    private BlobStore blobStore;
    private Logger logger = Logger.NULL;

    @Override
    public void setBlobStores(BlobStore store, BlobStore unused) {
        this.blobStore = store;
    }

    @Override
    public BlobStore getSource() {
        return blobStore;
    }

    @Override
    public BlobStore getDestination() {
        return null;
    }

    @Override public BounceResult reconcileObject(String container, BounceStorageMetadata source, StorageMetadata
            destination) {
        return BounceResult.NO_OP;
    }

    @Override
    public Blob getBlob(String container, String blobName, GetOptions options) {
        return blobStore.getBlob(container, blobName, options);
    }

    @Override
    public Logger getLogger() {
        return logger;
    }
}
