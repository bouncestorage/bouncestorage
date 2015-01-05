/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import com.google.inject.AbstractModule;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.attr.ConsistencyModel;
import org.jclouds.blobstore.config.BlobStoreObjectModule;

public final class BounceBlobStoreContextModule extends AbstractModule {
    @Override
    protected void configure() {
        install(new BlobStoreObjectModule());
        bind(BlobStore.class).to(BounceBlobStore.class);
        bind(ConsistencyModel.class).toInstance(ConsistencyModel.EVENTUAL);
    }
}
