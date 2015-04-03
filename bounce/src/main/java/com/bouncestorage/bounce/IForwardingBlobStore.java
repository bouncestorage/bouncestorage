/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.ContainerAccess;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.domain.Location;

public interface IForwardingBlobStore extends BlobStore {
    BlobStore delegate();

    @Override
    default BlobStoreContext getContext() {
        return delegate().getContext();
    }

    @Override
    default BlobBuilder blobBuilder(String name) {
        return delegate().blobBuilder(name);
    }

    @Override
    default Set<? extends Location> listAssignableLocations() {
        return delegate().listAssignableLocations();
    }

    @Override
    default PageSet<? extends StorageMetadata> list() {
        return delegate().list();
    }

    @Override
    default boolean containerExists(String container) {
        return delegate().containerExists(container);
    }

    @Override
    default boolean createContainerInLocation(Location location,
            String container) {
        return createContainerInLocation(location, container, CreateContainerOptions.NONE);
    }

    @Override
    default boolean createContainerInLocation(Location location,
            String container, CreateContainerOptions createContainerOptions) {
        return delegate().createContainerInLocation(location, container,
                createContainerOptions);
    }

    @Override
    default ContainerAccess getContainerAccess(String container) {
        return delegate().getContainerAccess(container);
    }

    @Override
    default void setContainerAccess(String container, ContainerAccess
            containerAccess) {
        delegate().setContainerAccess(container, containerAccess);
    }

    @Override
    default PageSet<? extends StorageMetadata> list(String container) {
        return delegate().list(container);
    }

    @Override
    default PageSet<? extends StorageMetadata> list(String container,
            ListContainerOptions options) {
        return delegate().list(container, options);
    }

    @Override
    default void clearContainer(String container) {
        clearContainer(container, ListContainerOptions.NONE);
    }

    @Override
    default void clearContainer(String container, ListContainerOptions options) {
        delegate().clearContainer(container, options);
    }

    @Override
    default void deleteContainer(String container) {
        delegate().deleteContainer(container);
    }

    @Override
    default boolean deleteContainerIfEmpty(String container) {
        return delegate().deleteContainerIfEmpty(container);
    }

    @Override
    default boolean directoryExists(String container, String directory) {
        return delegate().directoryExists(container, directory);
    }

    @Override
    default void createDirectory(String container, String directory) {
        delegate().createDirectory(container, directory);
    }

    @Override
    default void deleteDirectory(String container, String directory) {
        delegate().deleteDirectory(container, directory);
    }

    @Override
    default boolean blobExists(String container, String name) {
        return delegate().blobExists(container, name);
    }

    @Override
    default String putBlob(String containerName, Blob blob) {
        return putBlob(containerName, blob, PutOptions.NONE);
    }

    @Override
    default String putBlob(String containerName, Blob blob,
            PutOptions putOptions) {
        return delegate().putBlob(containerName, blob, putOptions);
    }

    @Override
    default BlobMetadata blobMetadata(String container, String name) {
        return delegate().blobMetadata(container, name);
    }

    @Override
    default Blob getBlob(String containerName, String blobName) {
        return getBlob(containerName, blobName, GetOptions.NONE);
    }

    @Override
    default Blob getBlob(String containerName, String blobName,
            GetOptions getOptions) {
        return delegate().getBlob(containerName, blobName, getOptions);
    }

    @Override
    default void removeBlob(String container, String name) {
        delegate().removeBlob(container, name);
    }

    @Override
    default void removeBlobs(String container, Iterable<String> iterable) {
        delegate().removeBlobs(container, iterable);
    }

    @Override
    default BlobAccess getBlobAccess(String container, String name) {
        return delegate().getBlobAccess(container, name);
    }

    @Override
    default void setBlobAccess(String container, String name,
            BlobAccess access) {
        delegate().setBlobAccess(container, name, access);
    }

    @Override
    default long countBlobs(String container) {
        return countBlobs(container, ListContainerOptions.NONE);
    }

    @Override
    default long countBlobs(String container, ListContainerOptions options) {
        return delegate().countBlobs(container, options);
    }

    default void updateBlobMetadata(String containerName, String blobName, Map<String, String> userMetadata) {
        Blob blob = getBlob(containerName, blobName);
        Map<String, String> allMetadata = new HashMap<>(blob.getMetadata().getUserMetadata());
        allMetadata.putAll(userMetadata);
        blob.getMetadata().setUserMetadata(allMetadata);
        putBlob(containerName, blob);
    }

    @Override
    default String copyBlob(String fromContainer, String fromName, String toContainer, String toName, CopyOptions options) {
        return delegate().copyBlob(fromContainer, fromName, toContainer, toName, options);
    }
}
