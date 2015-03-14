/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import java.io.IOException;
import java.util.function.Predicate;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.BounceStorageMetadata;

import org.apache.commons.configuration.Configuration;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.PutOptions;

public interface BouncePolicy extends Predicate<StorageMetadata> {
    enum BounceResult {
        NO_OP,
        COPY,
        MOVE,
    }
    default void init(BounceService service, Configuration config) {
    }

    default BounceResult bounce(BounceBlobStore blobStore, String container, BounceStorageMetadata meta) throws
            IOException {
        return BounceResult.NO_OP;
    }

    void setBlobStores(BlobStore source, BlobStore destination);

    BlobStore getSource();
    BlobStore getDestination();

    Blob getBlob(String container, String blobName, GetOptions options);

    default String putBlob(String container, Blob blob, PutOptions options) {
        return getSource().putBlob(container, blob, options);
    }

    BounceResult reconcile(String container, BounceStorageMetadata metadata);
}
