/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import static org.jclouds.reflect.Reflection2.typeToken;

import java.net.URI;

import org.jclouds.apis.ApiMetadata;
import org.jclouds.apis.internal.BaseApiMetadata;

public final class EncryptedApiMetadata extends BaseApiMetadata {

    public EncryptedApiMetadata(Builder builder) {
        super(builder);
    }

    public EncryptedApiMetadata() {
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
            id("encrypted")
                    .name("Encrypted Blob Store")
                    .identityName("Unused")
                    .defaultIdentity("")
                    .defaultEndpoint("")
                    .version("1")
                    .view(typeToken(EncryptedBlobStoreContext.class))
                    .defaultModule(EncryptedBlobStoreContextModule.class)
                    .documentation(URI.create("http://www.jclouds.org/documentation/userguide/blobstore-guide"));
        }

        @Override
        public EncryptedApiMetadata build() {
            return new EncryptedApiMetadata(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
