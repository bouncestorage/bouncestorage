/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import com.google.auto.value.AutoValue;

import org.jclouds.blobstore.BlobStore;

@AutoValue
public abstract class ContainerPoolKey {
    public static ContainerPoolKey create(String endpoint, String providerId) {
        return new AutoValue_ContainerPoolKey(endpoint, providerId);
    }

    public static ContainerPoolKey create(BlobStore blobStore) {
        return new AutoValue_ContainerPoolKey(blobStore.getContext().unwrap().getProviderMetadata().getEndpoint(),
                blobStore.getContext().unwrap().getId());
    }

    public abstract String getEndpoint();
    public abstract String getProviderId();
}
