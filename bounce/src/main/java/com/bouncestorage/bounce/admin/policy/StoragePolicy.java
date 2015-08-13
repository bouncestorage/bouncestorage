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
import java.util.Map;
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

    @VisibleForTesting
    long currentSize;

    private long capacity;
    private Instant evictionTime;

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
        setEvictionTime(containerName);
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

        Instant objectDate = getInstant(sourceObject.getLastModified());
        if (!objectDate.isAfter(evictionTime)) {
            try {
                return maybeMoveObject(container, sourceObject, destinationObject);
            } catch (IOException e) {
                propagate(e);
            }
        }

        return super.reconcileObject(container, sourceObject, destinationObject);
    }

    private void setEvictionTime(String containerName) {
        currentSize = 0;
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

        for (Map.Entry<Instant, Long> entry : sizeHistogram.entrySet()) {
            delta -= entry.getValue();
            if (delta < 0) {
                return entry.getKey();
            }
        }
        return Instant.MAX;
    }

    private Instant getInstant(Date date) {
        Instant value = date.toInstant().truncatedTo(ChronoUnit.DAYS);
        return value;
    }

    @VisibleForTesting
    public long getCapacity() {
        return capacity;
    }
}
