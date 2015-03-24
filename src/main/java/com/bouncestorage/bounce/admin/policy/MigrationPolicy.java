/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static com.google.common.base.Throwables.propagate;

import java.io.IOException;
import java.util.TreeMap;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.internal.PageSetImpl;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;

public final class MigrationPolicy extends BouncePolicy {
    // The policy implements migration from "source" to "destination"

    @Override
    public Blob getBlob(String container, String blobName, GetOptions options) {
        Blob blob = getDestination().getBlob(container, blobName, options);
        if (blob == null) {
            blob = getSource().getBlob(container, blobName, options);
        }

        return blob;
    }

    @Override
    public String putBlob(String container, Blob blob, PutOptions options) {
        return getDestination().putBlob(container, blob, options);
    }

    @Override
    public BounceResult reconcileObject(String container, BounceStorageMetadata sourceObject, StorageMetadata
            destinationObject) {
        if ((sourceObject == null) && (destinationObject == null)) {
            throw new AssertionError("At least one of source or destination objects must be non-null");
        }

        if (sourceObject.getRegions().equals(BounceBlobStore.NEAR_ONLY)) {
            return BounceResult.NO_OP;
        }
        if (sourceObject.getRegions().equals(BounceBlobStore.FAR_ONLY)) {
            return moveObject(container, sourceObject);
        }

        BlobMetadata sourceMeta = getSource().blobMetadata(container, sourceObject.getName());
        BlobMetadata destinationMeta = getDestination().blobMetadata(container, destinationObject.getName());
        if (sourceMeta == null && destinationMeta != null) {
            return BounceResult.NO_OP;
        } else if (sourceMeta != null && destinationMeta == null) {
            return moveObject(container, sourceMeta);
        }

        if (sourceMeta.getETag().equalsIgnoreCase(destinationMeta.getETag())) {
            getSource().removeBlob(container, sourceMeta.getName());
            return BounceResult.REMOVE;
        } else {
            logger.warn("Different objects with the same name: {}", sourceMeta.getName());
            return BounceResult.NO_OP;
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
                    contents.put(sourceName, new BounceStorageMetadata(sourceMeta, BounceBlobStore.FAR_ONLY));
                    sourceMeta = com.bouncestorage.bounce.Utils.getNextOrNull(sourcePage);
                } else if (compare == 0) {
                    contents.put(sourceName, new BounceStorageMetadata(sourceMeta, BounceBlobStore.EVERYWHERE));
                    sourceMeta = Utils.getNextOrNull(sourcePage);
                    destinationMeta = Utils.getNextOrNull(destinationPage);
                } else {
                    contents.put(destinationName, new BounceStorageMetadata(destinationMeta, BounceBlobStore
                            .NEAR_ONLY));
                    destinationMeta = Utils.getNextOrNull(destinationPage);
                }
            } else if (sourceMeta == null) {
                contents.put(destinationMeta.getName(), new BounceStorageMetadata(destinationMeta, BounceBlobStore
                        .NEAR_ONLY));
                destinationMeta = Utils.getNextOrNull(destinationPage);
            } else {
                contents.put(sourceMeta.getName(), new BounceStorageMetadata(sourceMeta, BounceBlobStore.FAR_ONLY));
                sourceMeta = Utils.getNextOrNull(sourcePage);
            }
        }

        if (sourceMeta != null || destinationMeta != null) {
            return new PageSetImpl<>(contents.values(), contents.lastKey());
        }
        return new PageSetImpl<>(contents.values(), null);
    }

    @Override
    public ImmutableSet<BlobStore> getCheckedStores() {
        return ImmutableSet.of(getSource(), getDestination());
    }

    private BounceResult moveObject(String container, StorageMetadata objectMetadata) {
        try {
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
}
