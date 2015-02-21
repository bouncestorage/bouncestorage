/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Resource;

import com.google.inject.Inject;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.*;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.domain.Location;
import org.jclouds.logging.Logger;
import org.jclouds.providers.ProviderMetadata;

public final class BounceBlobStore implements BlobStore {

    public static final String STORE_PROPERTY_1 = "bounce.store.properties.1";
    public static final String STORE_PROPERTY_2 = "bounce.store.properties.2";

    @Resource
    private Logger logger = Logger.NULL;

    private BlobStoreContext context;
    private BlobStore nearStore;
    private BlobStore farStore;

    @Inject
    BounceBlobStore(BlobStoreContext context, ProviderMetadata providerMetadata) {
        this.context = checkNotNull(context);
        Properties properties = providerMetadata.getDefaultProperties();

        initStores(Utils.extractProperties(properties, STORE_PROPERTY_1 + "."),
                Utils.extractProperties(properties, STORE_PROPERTY_2 + "."));
    }

    private void initStores(Properties prop1, Properties prop2) {
        this.nearStore = Utils.storeFromProperties(checkNotNull(prop1));
        this.farStore = Utils.storeFromProperties(checkNotNull(prop2));
    }

    BlobStore getNearStore() {
        return nearStore;
    }

    BlobStore getFarStore() {
        return farStore;
    }

    @Override
    public BlobStoreContext getContext() {
        return context;
    }

    @Override
    public BlobBuilder blobBuilder(String s) {
        return nearStore.blobBuilder(s);
    }

    @Override
    public Set<? extends Location> listAssignableLocations() {
        return farStore.listAssignableLocations();
    }

    @Override
    public PageSet<? extends StorageMetadata> list() {
        return nearStore.list();
    }

    @Override
    public boolean containerExists(String s) {
        return nearStore.containerExists(s);
    }

    @Override
    public boolean createContainerInLocation(Location location, String s) {
        return createContainerInLocation(location, s, CreateContainerOptions.NONE);
    }

    @Override
    public boolean createContainerInLocation(Location location, String s, CreateContainerOptions createContainerOptions) {
        return farStore.createContainerInLocation(location, s, createContainerOptions) |
                nearStore.createContainerInLocation(location, s);
    }

    @Override
    public ContainerAccess getContainerAccess(String s) {
        return nearStore.getContainerAccess(s);
    }

    @Override
    public void setContainerAccess(String s, ContainerAccess containerAccess) {
        nearStore.setContainerAccess(s, containerAccess);
        farStore.setContainerAccess(s, containerAccess);
    }

    @Override
    public PageSet<? extends StorageMetadata> list(String s) {
        return nearStore.list(s);
    }

    @Override
    public PageSet<? extends StorageMetadata> list(String s, ListContainerOptions listContainerOptions) {
        return nearStore.list(s, listContainerOptions);
    }

    @Override
    public void clearContainer(String s) {
        nearStore.clearContainer(s);
        farStore.clearContainer(s);
    }

    @Override
    public void clearContainer(String s, ListContainerOptions listContainerOptions) {
        nearStore.clearContainer(s, listContainerOptions);
        farStore.clearContainer(s, listContainerOptions);
    }

    @Override
    public void deleteContainer(String s) {
        nearStore.deleteContainer(s);
        farStore.deleteContainer(s);
    }

    @Override
    public boolean deleteContainerIfEmpty(String s) {
        boolean deleted = nearStore.deleteContainerIfEmpty(s);
        if (deleted) {
            farStore.deleteContainer(s);
        }
        return deleted;
    }

    @Override
    public boolean directoryExists(String s, String s1) {
        return nearStore.directoryExists(s, s1);
    }

    @Override
    public void createDirectory(String s, String s1) {
        nearStore.createDirectory(s, s1);
    }

    @Override
    public void deleteDirectory(String s, String s1) {
        nearStore.deleteDirectory(s, s1);
        farStore.deleteDirectory(s, s1);
    }

    @Override
    public boolean blobExists(String s, String s1) {
        return nearStore.blobExists(s, s1);
    }

    @Override
    public String putBlob(String containerName, Blob blob) {
        return putBlob(containerName, blob, PutOptions.NONE);
    }

    @Override
    public String putBlob(String containerName, Blob blob, PutOptions putOptions) {
        String etag = nearStore.putBlob(containerName, blob, putOptions);
        farStore.removeBlob(containerName, blob.getMetadata().getName());
        return etag;
    }

    @Override
    public BlobMetadata blobMetadata(String s, String s1) {
        BlobMetadata meta = nearStore.blobMetadata(s, s1);
        if (meta != null && BounceLink.isLink(meta)) {
            try {
                return BounceLink.fromBlob(nearStore.getBlob(s, s1)).getBlobMetadata();
            } catch (IOException e) {
                logger.error(e, "An error occurred while loading the metadata for blob %s in container %s",
                        s, s1);
                throw propagate(e);
            }
        } else {
            return meta;
        }
    }

    @Override
    public Blob getBlob(String containerName, String blobName) {
        return getBlob(containerName, blobName, GetOptions.NONE);
    }

    @Override
    public Blob getBlob(String containerName, String blobName, GetOptions getOptions) {
        BlobMetadata meta = nearStore.blobMetadata(containerName, blobName);
        if (BounceLink.isLink(meta)) {
            return farStore.getBlob(containerName, blobName, getOptions);
        } else {
            return nearStore.getBlob(containerName, blobName, getOptions);
        }
    }

    @Override
    public void removeBlob(String s, String s1) {
        nearStore.removeBlob(s, s1);
        farStore.removeBlob(s, s1);
    }

    @Override
    public void removeBlobs(String s, Iterable<String> iterable) {
        nearStore.removeBlobs(s, iterable);
        farStore.removeBlobs(s, iterable);
    }

    @Override
    public BlobAccess getBlobAccess(String container, String name) {
        return nearStore.getBlobAccess(container, name);
    }

    @Override
    public void setBlobAccess(String container, String name, BlobAccess access) {
        nearStore.setBlobAccess(container, name, access);
        farStore.setBlobAccess(container, name, access);
    }

    @Override
    public long countBlobs(String s) {
        return nearStore.countBlobs(s);
    }

    @Override
    public long countBlobs(String s, ListContainerOptions listContainerOptions) {
        return nearStore.countBlobs(s, listContainerOptions);
    }

    public boolean isLink(String containerName, String blobName) {
        return BounceLink.isLink(nearStore.blobMetadata(containerName, blobName));
    }

    public void copyBlobAndCreateBounceLink(String containerName, String blobName)
            throws IOException {
        Blob blobFrom = Utils.copyBlob(nearStore, farStore, containerName,
                containerName, blobName);
        if (blobFrom == null) {
            return;
        }
        BounceLink link = new BounceLink(Optional.of(blobFrom.getMetadata()));
        nearStore.putBlob(containerName, link.toBlob(nearStore));
    }

    public void takeOver(String containerName) throws IOException {
        // TODO: hook into move service to enable parallelism and cancellation
        for (StorageMetadata sm : Utils.crawlBlobStore(farStore,
                containerName)) {
            BlobMetadata metadata = farStore.blobMetadata(containerName,
                    sm.getName());
            BounceLink link = new BounceLink(Optional.of(metadata));
            nearStore.putBlob(containerName, link.toBlob(nearStore));
        }
    }

    /**
     * Sanity check that near store and far store are in sync, if they aren't,
     * we need to perform takeover.

     * @return true if the near store and farstore are in sync
     */
    public boolean sanityCheck(String containerName) throws IOException {
        PageSet<? extends StorageMetadata> res = farStore.list(containerName);
        for (StorageMetadata sm : res) {
            BlobMetadata meta = blobMetadata(containerName, sm.getName());
            if (!Utils.equals(sm, meta)) {
                return false;
            }
        }

        return true;
    }


}
