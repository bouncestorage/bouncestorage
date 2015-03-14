/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.google.auto.service.AutoService;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.StorageMetadata;

@AutoService(BouncePolicy.class)
public final class MoveEverythingPolicy extends MovePolicy {
    private BlobStore near;
    private BlobStore far;

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
    public String toString() {
        return "everything";
    }

    @Override
    public boolean test(StorageMetadata metadata) {
        return true;
    }

    @Override
    public BounceResult reconcile(String container, BounceStorageMetadata metadata) {
        return null;
    }
}
