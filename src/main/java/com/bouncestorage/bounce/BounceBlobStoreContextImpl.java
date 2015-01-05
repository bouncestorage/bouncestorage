/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import javax.inject.Inject;

import com.google.common.reflect.TypeToken;

import org.jclouds.Context;
import org.jclouds.blobstore.BlobRequestSigner;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.attr.ConsistencyModel;
import org.jclouds.blobstore.internal.BlobStoreContextImpl;
import org.jclouds.location.Provider;
import org.jclouds.rest.Utils;

/**
 * Created by khc on 1/2/15.
 */
public final class BounceBlobStoreContextImpl extends BlobStoreContextImpl implements BounceBlobStoreContext {

    @Inject
    public BounceBlobStoreContextImpl(@Provider Context backend, @Provider TypeToken<? extends Context> backendType, Utils utils, ConsistencyModel consistencyModel, BlobStore blobStore, BlobRequestSigner blobRequestSigner) {
        super(backend, backendType, utils, consistencyModel, blobStore, blobRequestSigner);
    }

    @Override
    public void close() {
        super.close();
        BounceBlobStore store = (BounceBlobStore) getBlobStore();
        store.getNearStore().getContext().close();
        store.getFarStore().getContext().close();
    }

}
