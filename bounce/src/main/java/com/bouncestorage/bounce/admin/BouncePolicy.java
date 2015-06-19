/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinTask;

import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.IForwardingBlobStore;
import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.configuration.Configuration;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.domain.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BouncePolicy implements IForwardingBlobStore {
    public enum BounceResult {
        NO_OP,
        COPY,
        MOVE,
        REMOVE,
        LINK,
    }

    protected Logger logger = LoggerFactory.getLogger(getClass());
    protected BounceApplication app;
    protected boolean takeOverInProcess;
    protected ForkJoinTask<?> takeOverFuture;

    private BlobStore sourceBlobStore;
    private BlobStore destinationBlobStore;

    public void init(BounceApplication bounceApplication, Configuration config) {
        this.app = requireNonNull(bounceApplication);
    }

    public final void setBlobStores(BlobStore source, BlobStore destination) {
        sourceBlobStore = source;
        destinationBlobStore = destination;
    }

    // Not final as we need to use this method with Mockito
    public BlobStore getSource() {
        return sourceBlobStore;
    }

    public final BlobStore getDestination() {
        return destinationBlobStore;
    }

    @Override
    public boolean createContainerInLocation(Location location, String container, CreateContainerOptions options) {
        return getDestination().createContainerInLocation(location, container, options) |
                getSource().createContainerInLocation(location, container, options);
    }

    @Override
    public BlobStore delegate() {
        return getSource();
    }

    public abstract BounceResult reconcileObject(String container, BounceStorageMetadata sourceObject, StorageMetadata
            destinationObject);

    public void prepareBounce(String containerName) {
    }

    public void takeOver(String containerName) {
    }

    @VisibleForTesting
    public void waitForTakeOver() throws ExecutionException, InterruptedException {
        takeOverFuture.get();
    }

    @Override
    public void deleteContainer(String containerName) {
        getSource().deleteContainer(containerName);
        getDestination().deleteContainer(containerName);
    }

    /**
     * Sanity check that near store and far store are in sync, if they aren't,
     * we need to perform takeover.

     * @return true if the near store and farstore are in sync
     */
    public boolean sanityCheck(String containerName) {
        return true;
    }
}
