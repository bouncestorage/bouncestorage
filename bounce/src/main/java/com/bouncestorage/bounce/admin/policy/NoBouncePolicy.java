/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import java.util.stream.Collectors;

import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.google.auto.service.AutoService;

import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.internal.PageSetImpl;
import org.jclouds.blobstore.options.ListContainerOptions;

@AutoService(BouncePolicy.class)
public class NoBouncePolicy extends BouncePolicy {
    @Override
    public final PageSet<? extends StorageMetadata> list(String containerName, ListContainerOptions
            listContainerOptions) {
        PageSet<? extends StorageMetadata> listResults = delegate().list(containerName, listContainerOptions);
        return new PageSetImpl<>(listResults.stream().map(m -> new BounceStorageMetadata(m, BounceStorageMetadata.NEAR_ONLY))
                .collect(Collectors.toList()), listResults.getNextMarker());
    }

    @Override
    public final BounceResult reconcileObject(String container, BounceStorageMetadata sourceObject, StorageMetadata
            destinationObject) {
        return BounceResult.NO_OP;
    }
}
