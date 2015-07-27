/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static com.google.common.base.Throwables.propagate;

import java.io.IOException;
import java.util.Set;
import java.util.TreeMap;

import javax.ws.rs.ServiceUnavailableException;

import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.bouncestorage.bounce.utils.ReconcileLocker;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.KeyNotFoundException;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.internal.PageSetImpl;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;

@AutoService(BouncePolicy.class)
public final class MigrationPolicy extends BouncePolicy {
    // The policy implements migration from "source" to "destination"
    private static final Set<BounceStorageMetadata.Region> DESTINATION = BounceStorageMetadata.NEAR_ONLY;
    private static final Set<BounceStorageMetadata.Region> SOURCE = BounceStorageMetadata.FAR_ONLY;
    private ReconcileLocker reconcileLocker = new ReconcileLocker();

    @Override
    public Blob getBlob(String container, String blobName, GetOptions options) {
        for (BlobStore blobStore : getCheckedStores()) {
            Blob blob = blobStore.getBlob(container, blobName, options);
            if (blob != null) {
                return blob;
            }
        }
        return null;
    }

    @Override
    public String putBlob(String container, Blob blob, PutOptions options) {
        Object lock = reconcileLocker.lockObject(container, blob.getMetadata().getName(), false);
        if (lock == null) {
            throw new ServiceUnavailableException("reconciling this object", 5L);
        }
        try {
            return getDestination().putBlob(container, blob, options);
        } finally {
            reconcileLocker.unlockObject(lock, false);
        }
    }

    @Override
    public BounceResult reconcileObject(String container, BounceStorageMetadata sourceObject, StorageMetadata
            destinationObject) {
        if ((sourceObject == null) && (destinationObject == null)) {
            throw new AssertionError("At least one of source or destination objects must be non-null");
        }
        String blobName = sourceObject == null ? destinationObject.getName() : sourceObject.getName();
        logger.debug("reconciling {}", blobName);

        if (sourceObject.getRegions().equals(DESTINATION)) {
            return BounceResult.NO_OP;
        }

        Object lock = reconcileLocker.lockObject(container, blobName, true);
        if (lock == null) {
            // not able to lock key, another PUT is in operation, so we can just skip
            // this. note that we should not delete the object from source store,
            // because the PUT may fail
            return BounceResult.NO_OP;
        }
        try {
            if (sourceObject.getRegions().equals(SOURCE)) {
                return moveObject(container, sourceObject);
            }

            BlobMetadata sourceMeta = getSource().blobMetadata(container, sourceObject.getName());
            BlobMetadata destinationMeta = getDestination().blobMetadata(container, destinationObject.getName());
            if (sourceMeta == null && destinationMeta != null) {
                return BounceResult.NO_OP;
            } else if (sourceMeta != null && destinationMeta == null) {
                return moveObject(container, sourceMeta);
            }

            if (Utils.eTagsEqual(sourceMeta.getETag(), destinationMeta.getETag())) {
                getSource().removeBlob(container, sourceMeta.getName());
                return BounceResult.REMOVE;
            } else {
                if (sourceMeta.getLastModified().compareTo(destinationMeta.getLastModified()) > 0) {
                    logger.warn("Different objects with the same name: {}", sourceMeta.getName());
                }
                return BounceResult.NO_OP;
            }
        } finally {
            reconcileLocker.unlockObject(lock, true);
        }
    }

    @Override
    public PageSet<? extends StorageMetadata> list(String containerName, ListContainerOptions listContainerOptions) {
        PeekingIterator<StorageMetadata> sourcePage = Iterators.peekingIterator(
                Utils.crawlBlobStore(getSource(), containerName, listContainerOptions)
                        .iterator());
        PeekingIterator<StorageMetadata> destinationPage = Iterators.peekingIterator(
                Utils.crawlBlobStore(getDestination(), containerName, listContainerOptions)
                        .iterator());
        TreeMap<String, BounceStorageMetadata> contents = new TreeMap<>();
        int maxResults = listContainerOptions.getMaxResults() == null ?
                1000 : listContainerOptions.getMaxResults();

        StorageMetadata sourceMeta = Utils.getNextOrNull(sourcePage);
        StorageMetadata destinationMeta = Utils.getNextOrNull(destinationPage);
        while (contents.size() < maxResults && (sourceMeta != null || destinationMeta != null)) {
            if (sourceMeta != null && destinationMeta != null) {
                String sourceName = sourceMeta.getName();
                String destinationName = destinationMeta.getName();
                int compare = sourceName.compareTo(destinationName);
                if (compare < 0) {
                    contents.put(sourceName, new BounceStorageMetadata(sourceMeta, SOURCE));
                    sourceMeta = com.bouncestorage.bounce.Utils.getNextOrNull(sourcePage);
                } else if (compare == 0) {
                    contents.put(sourceName, new BounceStorageMetadata(sourceMeta, BounceStorageMetadata.EVERYWHERE));
                    sourceMeta = Utils.getNextOrNull(sourcePage);
                    destinationMeta = Utils.getNextOrNull(destinationPage);
                } else {
                    contents.put(destinationName, new BounceStorageMetadata(destinationMeta, DESTINATION));
                    destinationMeta = Utils.getNextOrNull(destinationPage);
                }
            } else if (sourceMeta == null) {
                contents.put(destinationMeta.getName(), new BounceStorageMetadata(destinationMeta, DESTINATION));
                destinationMeta = Utils.getNextOrNull(destinationPage);
            } else {
                contents.put(sourceMeta.getName(), new BounceStorageMetadata(sourceMeta, SOURCE));
                sourceMeta = Utils.getNextOrNull(sourcePage);
            }
        }

        if (sourceMeta != null || destinationMeta != null) {
            return new PageSetImpl<>(contents.values(), contents.lastKey());
        }
        return new PageSetImpl<>(contents.values(), null);
    }

    @Override
    public BlobMetadata blobMetadata(String s, String s1) {
        for (BlobStore blobStore : getCheckedStores()) {
            BlobMetadata meta = blobStore.blobMetadata(s, s1);
            if (meta != null) {
                return meta;
            }
        }
        return null;
    }

    @Override
    public void removeBlob(String s, String s1) {
        for (BlobStore blobStore : getCheckedStores()) {
            blobStore.removeBlob(s, s1);
        }
    }

    @Override
    public void removeBlobs(String s, Iterable<String> iterable) {
        for (BlobStore blobStore : getCheckedStores()) {
            blobStore.removeBlobs(s, iterable);
        }
    }

    @Override
    public String copyBlob(String fromContainer, String fromName, String toContainer, String toName, CopyOptions options) {
        try {
            return getDestination().copyBlob(fromContainer, fromName, toContainer, toName, options);
        } catch (KeyNotFoundException e) {
            // ignored
        }

        return getSource().copyBlob(fromContainer, fromName, toContainer, toName, options);
    }

    private ImmutableList<BlobStore> getCheckedStores() {
        return ImmutableList.of(getDestination(), getSource());
    }

    private BounceResult moveObject(String container, StorageMetadata objectMetadata) {
        try {
            logger.debug("copying blob {}", objectMetadata.getName());
            Blob blob = Utils.copyBlob(getSource(), getDestination(), container, container, objectMetadata.getName());
            if (blob == null) {
                throw new RuntimeException("Failed to move the blob: " + objectMetadata.getName());
            }
            getSource().removeBlob(container, objectMetadata.getName());
            return BounceResult.MOVE;
        } catch (IOException e) {
            throw propagate(e);
        }
    }

    @Override
    public BlobStore delegate() {
        return getDestination();
    }
}
