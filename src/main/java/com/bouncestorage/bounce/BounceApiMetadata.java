package com.bouncestorage.bounce;

import org.jclouds.apis.ApiMetadata;
import org.jclouds.apis.internal.BaseApiMetadata;
import org.jclouds.blobstore.BlobStoreContext;

import java.net.URI;

/**
 * Created by khc on 1/1/15.
 */
public class BounceApiMetadata extends BaseApiMetadata {

    public static Builder builder() {
        return new Builder();
    }

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

    public static class Builder extends BaseApiMetadata.Builder<Builder> {

        protected Builder() {
            id("bounce")
            .name("Bouncing Blob Store")
            .identityName("Unused")
            .defaultEndpoint("http://localhost")
            .defaultIdentity(System.getProperty("user.name"))
            .defaultCredential("bar")
            .version("1")
            .view(BlobStoreContext.class)
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
