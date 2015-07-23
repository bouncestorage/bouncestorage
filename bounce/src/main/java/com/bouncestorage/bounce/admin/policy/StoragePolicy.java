/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static com.google.common.base.Throwables.propagate;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.TreeMap;

import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.admin.BounceApplication;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.configuration.Configuration;
import org.jclouds.blobstore.domain.StorageMetadata;

@AutoService(BouncePolicy.class)
public class StoragePolicy extends WriteBackPolicy {
    public static final String CAPACITY_SETTING = "capacity";

    protected long capacity;
    protected Instant evictionTime;

    @VisibleForTesting
    long currentSize;

    @Override
    public void init(BounceApplication app, Configuration configuration) {
        super.init(app, configuration);
        capacity = configuration.getLong(CAPACITY_SETTING);
        evictDelay = Duration.ofHours(-1);
        copyDelay = Duration.ofHours(-1);
    }

    /**
     * The storage policy evicts objects subject to a Last Recently Modified policy (as that is the only information
     * we can access from the object store). The
     * @param containerName
     */
    @Override
    public void prepareBounce(String containerName) {
        currentSize = 0;
        evictionTime = null;
        setEvictionTime(containerName);
    }

    protected boolean shouldEvict(String container, String blob, StorageMetadata meta) {
        Instant objectDate = getInstant(meta.getLastModified());
        return !objectDate.isAfter(evictionTime);
    }

    @Override
    public BounceResult reconcileObject(String container, BounceStorageMetadata sourceObject,
                                        StorageMetadata destinationObject) {
        if (sourceObject == null) {
            return super.reconcileObject(container, sourceObject, destinationObject);
        }

        if (currentSize < capacity) {
            return super.reconcileObject(container, sourceObject, destinationObject);
        }

        BounceResult res;
        if (shouldEvict(container, sourceObject.getName(), sourceObject)) {
            try {
                res = maybeMoveObject(container, sourceObject, destinationObject);
            } catch (IOException e) {
                throw propagate(e);
            }
        } else {
            res = super.reconcileObject(container, sourceObject, destinationObject);
        }

        if (res == BounceResult.MOVE || res == BounceResult.REMOVE || res == BounceResult.LINK) {
            currentSize -= sourceObject.getSize();
        }

        return res;
    }

    protected void setEvictionTime(String containerName) {
        // The MultiSet is limited to int, hence using the Map here
        TreeMap<Instant, Long> sizeHistogram = new TreeMap<>();

        // Populate the histogram
        for (StorageMetadata meta : Utils.crawlBlobStore(getSource(), containerName)) {
            Instant date = getInstant(meta.getLastModified());
            sizeHistogram.put(date, sizeHistogram.getOrDefault(date, 0L) + meta.getSize());
            currentSize += meta.getSize();
        }

        evictionTime = getEvictionTime(sizeHistogram);
        logger.debug("Set eviction time for " + containerName + ": " + evictionTime);
    }

    Instant getEvictionTime(TreeMap<Instant, Long> sizeHistogram) {
        long delta = currentSize - capacity;
        if (delta < 0) {
            return Instant.MIN;
        }

        for (Instant key : sizeHistogram.keySet()) {
            delta -= sizeHistogram.get(key);
            if (delta < 0) {
                return key;
            }
        }
        return Instant.MAX;
    }

    Instant getInstant(Date date) {
        Instant value = date.toInstant().truncatedTo(ChronoUnit.DAYS);
        return value;
    }
}
