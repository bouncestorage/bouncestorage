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

import static org.jclouds.reflect.Reflection2.typeToken;

import java.net.URI;

import org.jclouds.apis.ApiMetadata;
import org.jclouds.apis.internal.BaseApiMetadata;

public final class BounceApiMetadata extends BaseApiMetadata {

    public BounceApiMetadata(Builder builder) {
        super(builder);
    }

    public BounceApiMetadata() {
        super(new Builder());
    }

    @Override
    public ApiMetadata.Builder<?> toBuilder() {
        return new Builder().fromApiMetadata(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder extends BaseApiMetadata.Builder<Builder> {

        protected Builder() {
            id("bounce")
                .name("Bouncing Blob Store")
                .identityName("Unused")
                .defaultEndpoint("http://localhost")
                .defaultIdentity(System.getProperty("user.name"))
                .defaultCredential("bar")
                .version("1")
                .view(typeToken(BounceBlobStoreContext.class))
                .defaultModule(BounceBlobStoreContextModule.class)
                .documentation(URI.create("http://www.jclouds.org/documentation/userguide/blobstore-guide"));
        }

        @Override
        public BounceApiMetadata build() {
            return new BounceApiMetadata(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
