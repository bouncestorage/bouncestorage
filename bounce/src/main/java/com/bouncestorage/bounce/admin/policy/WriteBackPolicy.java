/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Throwables.propagate;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.bouncestorage.bounce.admin.BounceService;
import com.google.auto.service.AutoService;

import org.apache.commons.configuration.Configuration;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.PutOptions;

@AutoService(BouncePolicy.class)
public final class WriteBackPolicy extends MovePolicy {
    public static final String DURATION = "duration";
    private BounceService service;
    private Duration timeAgo;

    public void init(BounceService inService, Configuration config) {
        this.service = requireNonNull(inService);
        this.timeAgo = requireNonNull(Duration.parse(config.getString(DURATION)));
    }

    @Override
    public void onPut(String container, Blob blob, PutOptions options) {
        // TODO: Implement the background copy to the destination store
    }

    @Override
    public BounceResult reconcileObject(String container, BounceStorageMetadata sourceObject, StorageMetadata
            destinationObject) {
        if (sourceObject != null) {
            if (!isObjectExpired(sourceObject)) {
                try {
                    return maybeCopyObject(container, sourceObject, destinationObject);
                } catch (IOException e) {
                    propagate(e);
                }
            } else {
                try {
                    return maybeMoveObject(container, sourceObject, destinationObject);
                } catch (IOException e) {
                    throw propagate(e);
                }
            }
        }

        return maybeRemoveDestinationObject(container, destinationObject);
    }

    private boolean isObjectExpired(StorageMetadata metadata) {
        Instant now = service.getClock().instant();
        Instant then = metadata.getLastModified().toInstant();
        return now.minus(timeAgo).isAfter(then);
    }
}
