/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.utils.nil;

import static org.jclouds.reflect.Reflection2.typeToken;

import java.net.URI;

import org.jclouds.apis.ApiMetadata;
import org.jclouds.apis.internal.BaseApiMetadata;

public final class NullApiMetadata extends BaseApiMetadata {
    public NullApiMetadata(Builder builder) {
        super(builder);
    }

    public NullApiMetadata() {
        super(new Builder());
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public ApiMetadata.Builder<?> toBuilder() {
        return new Builder().fromApiMetadata(this);
    }

    public static final class Builder extends BaseApiMetadata.Builder<Builder> {

        protected Builder() {
            id("null")
                    .name("Null Blob Store")
                    .identityName("null")
                    .defaultEndpoint("http://localhost")
                    .defaultIdentity("null")
                    .defaultCredential("null")
                    .version("1")
                    .view(typeToken(NullBlobStoreContext.class))
                    .defaultModule(NullBlobStoreContextModule.class)
                    .documentation(URI.create("https://github.com/bouncestorage/bouncestorage"));
        }

        @Override
        public NullApiMetadata build() {
            return new NullApiMetadata(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }

}
