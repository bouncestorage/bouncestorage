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

import com.google.inject.AbstractModule;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.attr.ConsistencyModel;
import org.jclouds.blobstore.config.BlobStoreObjectModule;

/**
 * Created by khc on 1/1/15.
 */
public final class BounceBlobStoreContextModule extends AbstractModule {
    @Override
    protected void configure() {
        install(new BlobStoreObjectModule());
        bind(BlobStore.class).to(BounceBlobStore.class);
        bind(ConsistencyModel.class).toInstance(ConsistencyModel.EVENTUAL);
    }
}
