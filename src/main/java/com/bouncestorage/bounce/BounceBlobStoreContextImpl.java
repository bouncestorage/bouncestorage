/*
 * Copyright 2015 Bounce Storage <info@bouncestorage.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
