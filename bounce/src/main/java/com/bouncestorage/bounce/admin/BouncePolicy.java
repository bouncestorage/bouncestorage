/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import java.io.IOException;
import java.util.Optional;

import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.IForwardingBlobStore;
import com.bouncestorage.bounce.Utils;

import org.apache.commons.configuration.Configuration;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.PageSet;
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

    private BlobStore sourceBlobStore;
    private BlobStore destinationBlobStore;

    public void init(BounceApplication bounceApplication, Configuration config) {
    }

    public final void setBlobStores(BlobStore source, BlobStore destination) {
        sourceBlobStore = source;
        destinationBlobStore = destination;
    }

    public final BlobStore getSource() {
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

    public void takeOver(String containerName) throws IOException {
        // TODO: hook into move service to enable parallelism and cancellation
        for (StorageMetadata sm : Utils.crawlBlobStore(getDestination(),
                containerName)) {
            BlobMetadata metadata = getDestination().blobMetadata(containerName,
                    sm.getName());
            BounceLink link = new BounceLink(Optional.of(metadata));
            getSource().putBlob(containerName, link.toBlob(getSource()));
        }
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
    public boolean sanityCheck(String containerName) throws IOException {
        PageSet<? extends StorageMetadata> res = getDestination().list(containerName);
        for (StorageMetadata sm : res) {
            BlobMetadata meta = blobMetadata(containerName, sm.getName());
            if (!Utils.equalsOtherThanTime(sm, meta)) {
                return false;
            }
        }

        return true;
    }
}
