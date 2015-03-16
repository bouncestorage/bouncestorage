/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.time.Instant;

import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.bouncestorage.bounce.admin.BounceService;
import com.google.auto.service.AutoService;

import org.apache.commons.configuration.Configuration;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.logging.Logger;

@AutoService(BouncePolicy.class)
public final class LastModifiedTimePolicy extends MovePolicy {
    public static final String DURATION = "duration";
    private BounceService service;
    private Duration timeAgo;
    private BlobStore original;
    private BlobStore archive;
    private Logger logger = Logger.NULL;

    @Override
    public void setBlobStores(BlobStore originalStore, BlobStore archiveStore) {
        this.original = originalStore;
        this.archive = archiveStore;
    }

    @Override
    public BlobStore getSource() {
        return original;
    }

    @Override
    public BlobStore getDestination() {
        return archive;
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    public void init(BounceService inService, Configuration config) {
        this.service = requireNonNull(inService);
        this.timeAgo = requireNonNull(Duration.parse(config.getString(DURATION)));
    }

    @Override
    public BounceResult reconcileObject(String container, BounceStorageMetadata sourceObject, StorageMetadata
            destinationObject) {
        if (sourceObject == null) {
            return Utils.maybeRemoveObject(getDestination(), container, destinationObject);
        }

        BlobMetadata sourceMeta = getSource().blobMetadata(container, sourceObject.getName());
        if (BounceLink.isLink(sourceMeta)) {
            return BounceResult.NO_OP;
        }

        boolean expired = isObjectExpired(sourceObject);
        if (destinationObject == null && !expired) {
            return BounceResult.NO_OP;
        }

        boolean objectCopied = false;
        if (destinationObject != null) {
            BlobMetadata destinationMeta = getDestination().blobMetadata(container, destinationObject.getName());
            objectCopied = sourceMeta.getETag().equalsIgnoreCase(destinationMeta.getETag());
        }

        if (expired) {
            if (objectCopied) {
                return Utils.createBounceLink(this, sourceMeta);
            } else {
                return Utils.copyBlobAndCreateBounceLink(this, container, sourceMeta.getName());
            }
        }

        return Utils.maybeRemoveObject(getDestination(), container, destinationObject);
    }

    private boolean isObjectExpired(StorageMetadata metadata) {
        Instant now = service.getClock().instant();
        Instant then = metadata.getLastModified().toInstant();
        return now.minus(timeAgo).isAfter(then);
    }
}
