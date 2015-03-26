/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.time.Instant;

import com.bouncestorage.bounce.*;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.bouncestorage.bounce.admin.BounceService;
import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.configuration.Configuration;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.PutOptions;

@AutoService(BouncePolicy.class)
public final class LastModifiedTimePolicy extends MovePolicy {
    public static final String DURATION = "duration";
    private BounceService service;
    private Duration timeAgo;

    public void init(BounceService inService, Configuration config) {
        config.getKeys().forEachRemaining(s -> logger.info("policy: {} = {}", s, config.getString(s)));
        this.service = requireNonNull(inService);
        this.timeAgo = requireNonNull(Duration.parse(config.getString(DURATION)));
    }

    @Override
    public BounceResult reconcileObject(String container, BounceStorageMetadata sourceObject, StorageMetadata
            destinationObject) {
        if ((sourceObject == null) && (destinationObject == null)) {
            throw new AssertionError("At least one of source or destination objects must be non-null");
        }

        if (sourceObject == null) {
            return maybeRemoveDestinationObject(container, destinationObject);
        }

        if (sourceObject.getRegions().equals(BounceBlobStore.FAR_ONLY)) {
            return BounceResult.NO_OP;
        }

        boolean expired = isObjectExpired(sourceObject);
        if (!expired) {
            if (sourceObject.getRegions().equals(BounceBlobStore.NEAR_ONLY)) {
                // The far store object does not exist or is different from near store
                return maybeRemoveDestinationObject(container, sourceObject);
            }
            // The object was copied to the far store
            return BounceResult.NO_OP;
        } else {
            BlobMetadata sourceMetadata = getSource().blobMetadata(container, sourceObject.getName());
            if (sourceObject.getRegions().equals(BounceBlobStore.EVERYWHERE)) {
                if (!BounceLink.isLink(sourceMetadata)) {
                    return com.bouncestorage.bounce.Utils.createBounceLink(this, sourceMetadata);
                } else {
                    return BounceResult.NO_OP;
                }
            } else {
                return Utils.copyBlobAndCreateBounceLink(getSource(), getDestination(), container,
                        sourceMetadata.getName());
            }
        }
    }

    @VisibleForTesting
    public boolean isObjectExpired(StorageMetadata metadata) {
        Instant now = service.getClock().instant();
        Instant then = metadata.getLastModified().toInstant();
        return now.minus(timeAgo).isAfter(then);
    }

    @Override
    public void onPut(String container, Blob blob, PutOptions options) {
        return;
    }
}
