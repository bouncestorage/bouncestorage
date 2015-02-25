/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static java.util.Objects.requireNonNull;

import java.util.Date;
import java.util.Objects;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.Utils;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.jclouds.blobstore.domain.StorageMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FsckTask implements Runnable {
    public enum Result {
        NO_OP,
        COPIED,
        REMOVED,
        LINKED
    };
    private Logger logger = LoggerFactory.getLogger(getClass());
    private String container;
    private BounceService.BounceTaskStatus status;
    private BounceBlobStore bounceStore;

    FsckTask(BounceBlobStore bounceStore, String container, BounceService.BounceTaskStatus status) {
        this.bounceStore = requireNonNull(bounceStore);
        this.container = requireNonNull(container);
        this.status = requireNonNull(status);
    }

    private void updateStats(Result res) {
        switch (res) {
            case COPIED:
                status.copiedObjectCount.getAndIncrement();
                break;
            case REMOVED:
                status.removedObjectCount.getAndIncrement();
                break;
            case LINKED:
                status.movedObjectCount.getAndIncrement();
                break;
            default:
                throw new IllegalArgumentException(res.toString());
        }
    }

    @Override
    public void run() {
        PeekingIterator<StorageMetadata> farIter = Iterators.peekingIterator(
                Utils.crawlFarBlobStore(bounceStore, container).iterator());
        PeekingIterator<StorageMetadata> userIter = Iterators.peekingIterator(
                Utils.crawlBlobStore(bounceStore, container).iterator());

        while (farIter.hasNext() && userIter.hasNext()) {
            StorageMetadata far = farIter.peek();
            BounceStorageMetadata user = (BounceStorageMetadata) userIter.peek();
            int compare = far.getName().compareTo(user.getName());
            if (compare == 0) {
                // they are the same key
                if (!user.getETag().equals(far.getETag())) {
                    logger.warn("{} != {}", Objects.toString(user), Objects.toString(far));
                    logger.warn("{} != {}", user.getETag(), far.getETag());
                    reconcileObject(user, far);
                }

                farIter.next();
                userIter.next();
                status.totalObjectCount.getAndIncrement();
            } else if (compare < 0) {
                // near store missing this object
                reconcileObject(null, far);
                farIter.next();
            } else {
                // far store missing this object
                if (user.getRegions().contains(BounceBlobStore.Region.FAR)) {
                    reconcileObject(user, null);
                }
                userIter.next();
                status.totalObjectCount.getAndIncrement();
            }
        }

        userIter.forEachRemaining(meta -> status.totalObjectCount.getAndIncrement());
        farIter.forEachRemaining(meta -> reconcileObject(null, meta));

        status.endTime = new Date();
    }

    private void reconcileObject(BounceStorageMetadata user, StorageMetadata far) {
        try {
            Result res = bounceStore.reconcileObject(container, user, far);
            updateStats(res);
        } catch (Throwable e) {
            logger.error(String.format("could not reconcile %s",
                    user != null ? user.getName() : far.getName()), e);
            status.errorObjectCount.getAndIncrement();
        }
    }
}
