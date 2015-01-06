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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Module;

import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.*;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.domain.Location;
import org.jclouds.logging.Logger;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;

public final class BounceBlobStore implements BlobStore {

    public static final String STORE_PROPERTY_1 = "bounce.store.properties.1";
    public static final String STORE_PROPERTY_2 = "bounce.store.properties.2";

    @Resource
    private Logger logger = Logger.NULL;

    private BlobStoreContext context;
    private BlobStore nearStore;
    private BlobStore farStore;

    @Inject
    BounceBlobStore(BlobStoreContext context) {
        this.context = checkNotNull(context);
    }

    void initStores(Properties prop1, Properties prop2) {
        this.nearStore = storeFromProperties(checkNotNull(prop1));
        this.farStore = storeFromProperties(checkNotNull(prop2));
    }

    private static BlobStore storeFromProperties(Properties properties) {
        String provider = properties.getProperty(Constants.PROPERTY_PROVIDER);
        ContextBuilder builder = ContextBuilder
                .newBuilder(provider)
                .modules(ImmutableList.<Module>of(new SLF4JLoggingModule()))
                .overrides(properties);
        BlobStoreContext context = builder.build(BlobStoreContext.class);
        return context.getBlobStore();
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
    public String putBlob(String s, Blob blob) {
        String etag = nearStore.putBlob(s, blob);
        farStore.removeBlob(s, blob.getMetadata().getName());
        return etag;
    }

    @Override
    public String putBlob(String s, Blob blob, PutOptions putOptions) {
        String etag = nearStore.putBlob(s, blob, putOptions);
        farStore.removeBlob(s, blob.getMetadata().getName());
        return etag;
    }

    @Override
    public BlobMetadata blobMetadata(String s, String s1) {
        BlobMetadata meta = nearStore.blobMetadata(s, s1);
        if (BounceLink.isLink(meta)) {
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
    public Blob getBlob(String s, String s1) {
        Blob b = nearStore.getBlob(s, s1);
        if (BounceLink.isLink(b.getMetadata())) {
            return farStore.getBlob(s, s1);
        } else {
            return b;
        }
    }

    @Override
    public Blob getBlob(String s, String s1, GetOptions getOptions) {
        BlobMetadata meta = nearStore.blobMetadata(s, s1);
        if (BounceLink.isLink(meta)) {
            return farStore.getBlob(s, s1, getOptions);
        } else {
            return nearStore.getBlob(s, s1, getOptions);
        }
    }

    @Override
    public void removeBlob(String s, String s1) {
        nearStore.removeBlob(s, s1);
        farStore.removeBlob(s, s1);
    }

    @Override
    public long countBlobs(String s) {
        return nearStore.countBlobs(s);
    }

    @Override
    public long countBlobs(String s, ListContainerOptions listContainerOptions) {
        return nearStore.countBlobs(s, listContainerOptions);
    }

    public void copyBlobAndCreateBounceLink(String containerName, String blobName)
            throws IOException {
        Blob blobFrom = Utils.copyBlob(nearStore, farStore, containerName,
                containerName, blobName);
        BounceLink link = new BounceLink(Optional.of(blobFrom.getMetadata()));
        nearStore.putBlob(containerName, link.toBlob(nearStore));
    }
}
