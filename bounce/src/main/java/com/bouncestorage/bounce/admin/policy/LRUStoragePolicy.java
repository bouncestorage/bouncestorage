/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.PutOptions;

@AutoService(BouncePolicy.class)
public class LRUStoragePolicy extends StoragePolicy {
    @AutoValue
    abstract static class BlobObject {
        public static BlobObject create(String container, String blob) {
            return new AutoValue_LRUStoragePolicy_BlobObject(container, blob);
        }
        public abstract String getContainer();
        public abstract String getBlob();
    }

    @AutoValue
    abstract static class LRUValue {
        public static LRUValue create(Long accessTime, Long size) {
            return new AutoValue_LRUStoragePolicy_LRUValue(accessTime, size);
        }
        public abstract Long getAccessTime();
        public abstract Long getSize();
    }

    // https://gist.github.com/andrewgaul/7108880
    // 44 bytes overhead per entry (in addition to String and Long)
    // assuming 200 bytes of overhead total, that's 200MB / million objects
    private Map<BlobObject, LRUValue> lru = Collections.synchronizedMap(new LinkedHashMap<>(100000, 0.75f));
    private Instant lruEvictTo;

    private BlobObject getLRUKey(String containerName, String blobName) {
        return BlobObject.create(containerName, blobName);
    }

    @Override
    public void prepareBounce(String containerName) {
        lruEvictTo = null;
        super.prepareBounce(containerName);
    }

    @Override
    protected boolean shouldEvict(String container, String blob, StorageMetadata meta) {
        if (lruEvictTo != null) {
            LRUValue v = lru.get(getLRUKey(container, blob));
            if (v != null) {
                return !Instant.ofEpochMilli(v.getAccessTime()).isBefore(lruEvictTo);
            }
        }

        return super.shouldEvict(container, blob, meta);
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

        for (LRUValue v : lru.values()) {
            delta -= v.getSize();
            if (delta < 0) {
                logger.debug("evicting up to atime {}", v.getAccessTime());
                lruEvictTo = Instant.ofEpochMilli(v.getAccessTime());
                break;
            }
        }

        return Instant.MAX;
    }

    @Override
    protected void setEvictionTime(String containerName) {
        currentSize = 0;
        // The MultiSet is limited to int, hence using the Map here
        TreeMap<Instant, Long> sizeHistogram = new TreeMap<>();

        // Populate the histogram
        for (StorageMetadata meta : Utils.crawlBlobStore(getSource(), containerName)) {
            String blobName = meta.getName();
            if (!lru.containsKey(getLRUKey(containerName, blobName))) {
                Instant date = getInstant(meta.getLastModified());
                sizeHistogram.put(date, sizeHistogram.getOrDefault(date, 0L) + meta.getSize());
            }
            currentSize += meta.getSize();
        }

        evictionTime = getEvictionTime(sizeHistogram);
        logger.debug("Set eviction time for " + containerName + ": " + evictionTime);
    }


    @Override
    public String putBlob(String containerName, Blob blob, PutOptions options) {
        String eTag = super.putBlob(containerName, blob, options);
        // the mtime of this object just got updated, we can fall back to that
        lru.remove(getLRUKey(containerName, blob.getMetadata().getName()));
        return eTag;
    }

    @Override
    public void removeBlob(String container, String name) {
        super.removeBlob(container, name);
        lru.remove(getLRUKey(container, name));
    }

    @Override
    public Blob getBlob(String container, String blobName, GetOptions options) {
        Blob blob = super.getBlob(container, blobName, options);
        if (blob != null) {
            updateLRU(container, blobName, blob.getMetadata().getSize());
        }
        return blob;
    }

    @Override
    public BlobMetadata blobMetadata(String container, String blobName) {
        BlobMetadata meta = super.blobMetadata(container, blobName);
        if (meta != null) {
            updateLRU(container, blobName, meta.getSize());
        }
        return meta;
    }

    private void updateLRU(String container, String blobName, Long size) {
        BlobObject key = getLRUKey(container, blobName);
        // make sure lru get updated
        lru.remove(key);
        lru.put(key, LRUValue.create(System.currentTimeMillis(), size));
    }
}
