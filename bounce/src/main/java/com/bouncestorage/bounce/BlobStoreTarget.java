/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import static java.util.Objects.requireNonNull;

import org.jclouds.blobstore.BlobStore;

public class BlobStoreTarget implements IForwardingBlobStore {
    private final BlobStore delegate;
    private final String container;

    public BlobStoreTarget(final BlobStore delegate, final String container) {
        this.delegate = requireNonNull(delegate);
        this.container = requireNonNull(container);
    }

    @Override
    public BlobStore delegate() {
        return delegate;
    }

    @Override
    public String mapContainer(String vContainer) {
        return this.container;
    }
}
