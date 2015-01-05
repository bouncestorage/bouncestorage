/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
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
