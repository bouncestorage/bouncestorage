/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import com.google.common.collect.ForwardingObject;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.ContainerAccess;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.domain.Location;

public abstract class ForwardingBlobStore extends ForwardingObject
        implements BlobStore {
    private final BlobStore blobStore;

    public ForwardingBlobStore(BlobStore blobStore) {
        this.blobStore = checkNotNull(blobStore);
    }

    protected BlobStore delegate() {
        return blobStore;
    }

    @Override
    public BlobStoreContext getContext() {
        return delegate().getContext();
    }

    @Override
    public BlobBuilder blobBuilder(String name) {
        return delegate().blobBuilder(name);
    }

    @Override
    public Set<? extends Location> listAssignableLocations() {
        return delegate().listAssignableLocations();
    }

    @Override
    public PageSet<? extends StorageMetadata> list() {
        return delegate().list();
    }

    @Override
    public boolean containerExists(String container) {
        return delegate().containerExists(container);
    }

    @Override
    public boolean createContainerInLocation(Location location,
            String container) {
        return delegate().createContainerInLocation(location, container);
    }

    @Override
    public boolean createContainerInLocation(Location location,
            String container, CreateContainerOptions createContainerOptions) {
        return delegate().createContainerInLocation(location, container,
                createContainerOptions);
    }

    @Override
    public ContainerAccess getContainerAccess(String container) {
        return delegate().getContainerAccess(container);
    }

    @Override
    public void setContainerAccess(String container, ContainerAccess
            containerAccess) {
        delegate().setContainerAccess(container, containerAccess);
    }

    @Override
    public PageSet<? extends StorageMetadata> list(String container) {
        return delegate().list(container);
    }

    @Override
    public PageSet<? extends StorageMetadata> list(String container,
            ListContainerOptions options) {
        return delegate().list(container, options);
    }

    @Override
    public void clearContainer(String container) {
        delegate().clearContainer(container);
    }

    @Override
    public void clearContainer(String container, ListContainerOptions options) {
        delegate().clearContainer(container, options);
    }

    @Override
    public void deleteContainer(String container) {
        delegate().deleteContainer(container);
    }

    @Override
    public boolean deleteContainerIfEmpty(String container) {
        return delegate().deleteContainerIfEmpty(container);
    }

    @Override
    public boolean directoryExists(String container, String directory) {
        return delegate().directoryExists(container, directory);
    }

    @Override
    public void createDirectory(String container, String directory) {
        delegate().createDirectory(container, directory);
    }

    @Override
    public void deleteDirectory(String container, String directory) {
        delegate().deleteDirectory(container, directory);
    }

    @Override
    public boolean blobExists(String container, String name) {
        return delegate().blobExists(container, name);
    }

    @Override
    public String putBlob(String containerName, Blob blob) {
        return delegate().putBlob(containerName, blob);
    }

    @Override
    public String putBlob(String containerName, Blob blob,
            PutOptions putOptions) {
        return delegate().putBlob(containerName, blob, putOptions);
    }

    @Override
    public BlobMetadata blobMetadata(String container, String name) {
        return delegate().blobMetadata(container, name);
    }

    @Override
    public Blob getBlob(String containerName, String blobName) {
        return delegate().getBlob(containerName, blobName);
    }

    @Override
    public Blob getBlob(String containerName, String blobName,
            GetOptions getOptions) {
        return delegate().getBlob(containerName, blobName, getOptions);
    }

    @Override
    public void removeBlob(String container, String name) {
        delegate().removeBlob(container, name);
    }

    @Override
    public void removeBlobs(String container, Iterable<String> iterable) {
        delegate().removeBlobs(container, iterable);
    }

    @Override
    public BlobAccess getBlobAccess(String container, String name) {
        return delegate().getBlobAccess(container, name);
    }

    @Override
    public void setBlobAccess(String container, String name,
            BlobAccess access) {
        delegate().setBlobAccess(container, name, access);
    }

    @Override
    public long countBlobs(String container) {
        return delegate().countBlobs(container);
    }

    @Override
    public long countBlobs(String container, ListContainerOptions options) {
        return delegate().countBlobs(container, options);
    }
}
