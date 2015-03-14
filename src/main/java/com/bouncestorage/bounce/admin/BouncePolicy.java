/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import java.io.IOException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.BounceStorageMetadata;

import org.apache.commons.configuration.Configuration;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.internal.PageSetImpl;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.logging.Logger;

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

    Logger getLogger();

    BounceResult reconcile(String container, BounceStorageMetadata metadata);

    default PageSet<? extends StorageMetadata> list(String containerName, ListContainerOptions listContainerOptions) {
        PageSet<? extends StorageMetadata> listResults = getSource().list(containerName, listContainerOptions);
        return new PageSetImpl<>(listResults.stream().map(m -> new BounceStorageMetadata(m, BounceBlobStore.NEAR_ONLY))
                .collect(Collectors.toList()), listResults.getNextMarker());
    }
}
