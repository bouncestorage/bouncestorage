/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.utils.nil;

import com.google.inject.AbstractModule;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.attr.ConsistencyModel;
import org.jclouds.blobstore.config.BlobStoreObjectModule;

public final class NullBlobStoreContextModule extends AbstractModule {
    @Override
    protected void configure() {
        install(new BlobStoreObjectModule());
        bind(BlobStore.class).to(NullBlobStore.class);
        bind(ConsistencyModel.class).toInstance(ConsistencyModel.EVENTUAL);
    }
}
