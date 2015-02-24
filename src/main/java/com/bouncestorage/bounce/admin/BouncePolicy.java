/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import java.io.IOException;
import java.util.function.Predicate;

import com.bouncestorage.bounce.BounceBlobStore;

import org.apache.commons.configuration.Configuration;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageMetadata;

public interface BouncePolicy extends Predicate<StorageMetadata> {
    enum BounceResult {
        NO_OP,
        COPY,
        MOVE,
    }
    default void init(BounceService service, Configuration config) {
    }

    default BounceResult bounce(BlobMetadata meta, BounceBlobStore blobStore) throws IOException {
        return BounceResult.NO_OP;
    }
}
