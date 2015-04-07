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
import java.util.concurrent.TimeUnit;

import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.admin.BounceApplication;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.google.auto.service.AutoService;

import org.apache.commons.configuration.Configuration;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.PutOptions;

@AutoService(BouncePolicy.class)
public class WriteBackPolicy extends MovePolicy {
    public static final String COPY_DELAY = "copyDelay";
    public static final String EVICT_DELAY = "evictDelay";
    protected Duration copyDelay;
    protected Duration evictDelay;
    private boolean copy;
    private boolean immediateCopy;
    private boolean evict;

    @Override
    public String putBlob(String containerName, Blob blob, PutOptions options) {
        if (immediateCopy) {
            // TODO: implement immediate write back
        }
        String etag = super.putBlob(containerName, blob, options);
        String blobName = blob.getMetadata().getName();
        if (app != null) {
            app.executeBackgroundTask(() ->
                    Utils.copyBlob(getSource(), getDestination(), containerName, containerName, blobName),
                    copyDelay.getSeconds(), TimeUnit.SECONDS);
        }
        return etag;
    }

    public void init(BounceApplication app, Configuration config) {
        super.init(app, config);
        this.copyDelay = requireNonNull(Duration.parse(config.getString(COPY_DELAY)));
        this.copy = !copyDelay.isNegative();
        this.immediateCopy = copyDelay.isZero();
        this.evictDelay = requireNonNull(Duration.parse(config.getString(EVICT_DELAY)));
        this.evict = !evictDelay.isNegative();
    }

    @Override
    public BounceResult reconcileObject(String container, BounceStorageMetadata sourceObject, StorageMetadata
            destinationObject) {
        if (sourceObject != null) {
            try {
                if (evict && isObjectExpired(sourceObject, evictDelay)) {
                    return maybeMoveObject(container, sourceObject, destinationObject);
                } else if (copy && (immediateCopy || isObjectExpired(sourceObject, copyDelay))) {
                    return maybeCopyObject(container, sourceObject, destinationObject);
                }
            } catch (IOException e) {
                throw propagate(e);
            }

            return BounceResult.NO_OP;
        }

        return maybeRemoveDestinationObject(container, destinationObject);
    }

    protected boolean isObjectExpired(StorageMetadata metadata, Duration duration) {
        Instant now = app.getClock().instant();
        Instant then = metadata.getLastModified().toInstant();
        return !now.minus(duration).isBefore(then);
    }
}
