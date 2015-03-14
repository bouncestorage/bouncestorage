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

public abstract class BouncePolicy implements Predicate<StorageMetadata> {
    public enum BounceResult {
        NO_OP,
        COPY,
        MOVE,
    }

    private BlobStore sourceBlobStore;
    private BlobStore destinationBlobStore;

    public void init(BounceService service, Configuration config) {
    }

    public abstract BounceResult bounce(BounceBlobStore blobStore, String container, BounceStorageMetadata meta) throws
            IOException;

    public final void setBlobStores(BlobStore source, BlobStore destination) {
        sourceBlobStore = source;
        destinationBlobStore = destination;
    }

    public final BlobStore getSource() {
        return sourceBlobStore;
    }

    public final BlobStore getDestination() {
        return destinationBlobStore;
    }

    public abstract Blob getBlob(String container, String blobName, GetOptions options);

    public abstract String putBlob(String container, Blob blob, PutOptions options);
}
