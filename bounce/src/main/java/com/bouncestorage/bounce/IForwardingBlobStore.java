/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.ContainerAccess;
import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.domain.Location;
import org.jclouds.io.Payload;

public interface IForwardingBlobStore extends BlobStore {
    BlobStore delegate();
    default String mapContainer(String container) {
        return container;
    }

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
        return delegate().containerExists(mapContainer(container));
    }

    @Override
    default boolean createContainerInLocation(Location location,
            String container) {
        return createContainerInLocation(location, container, CreateContainerOptions.NONE);
    }

    @Override
    default boolean createContainerInLocation(Location location,
            String container, CreateContainerOptions createContainerOptions) {
        return delegate().createContainerInLocation(location, mapContainer(container),
                createContainerOptions);
    }

    @Override
    default ContainerAccess getContainerAccess(String container) {
        return delegate().getContainerAccess(mapContainer(container));
    }

    @Override
    default void setContainerAccess(String container, ContainerAccess
            containerAccess) {
        delegate().setContainerAccess(mapContainer(container), containerAccess);
    }

    @Override
    default PageSet<? extends StorageMetadata> list(String container) {
        return list(container, ListContainerOptions.NONE);
    }

    @Override
    default PageSet<? extends StorageMetadata> list(String container,
            ListContainerOptions options) {
        return delegate().list(mapContainer(container), options);
    }

    @Override
    default void clearContainer(String container) {
        clearContainer(container, ListContainerOptions.NONE);
    }

    @Override
    default void clearContainer(String container, ListContainerOptions options) {
        delegate().clearContainer(mapContainer(container), options);
    }

    @Override
    default void deleteContainer(String container) {
        delegate().deleteContainer(mapContainer(container));
    }

    @Override
    default boolean deleteContainerIfEmpty(String container) {
        return delegate().deleteContainerIfEmpty(mapContainer(container));
    }

    @Override
    default boolean directoryExists(String container, String directory) {
        return delegate().directoryExists(mapContainer(container), directory);
    }

    @Override
    default void createDirectory(String container, String directory) {
        delegate().createDirectory(mapContainer(container), directory);
    }

    @Override
    default void deleteDirectory(String container, String directory) {
        delegate().deleteDirectory(mapContainer(container), directory);
    }

    @Override
    default boolean blobExists(String container, String name) {
        return delegate().blobExists(mapContainer(container), name);
    }

    @Override
    default String putBlob(String containerName, Blob blob) {
        return putBlob(containerName, blob, PutOptions.NONE);
    }

    @Override
    default String putBlob(String containerName, Blob blob,
            PutOptions putOptions) {
        return delegate().putBlob(mapContainer(containerName), blob, putOptions);
    }

    @Override
    default BlobMetadata blobMetadata(String container, String name) {
        return delegate().blobMetadata(mapContainer(container), name);
    }

    @Override
    default Blob getBlob(String containerName, String blobName) {
        return getBlob(containerName, blobName, GetOptions.NONE);
    }

    @Override
    default Blob getBlob(String containerName, String blobName,
            GetOptions getOptions) {
        return delegate().getBlob(mapContainer(containerName), blobName, getOptions);
    }

    @Override
    default void removeBlob(String container, String name) {
        delegate().removeBlob(mapContainer(container), name);
    }

    @Override
    default void removeBlobs(String container, Iterable<String> iterable) {
        delegate().removeBlobs(mapContainer(container), iterable);
    }

    @Override
    default BlobAccess getBlobAccess(String container, String name) {
        return delegate().getBlobAccess(mapContainer(container), name);
    }

    @Override
    default void setBlobAccess(String container, String name,
            BlobAccess access) {
        delegate().setBlobAccess(mapContainer(container), name, access);
    }

    @Override
    default long countBlobs(String container) {
        return countBlobs(container, ListContainerOptions.NONE);
    }

    @Override
    default long countBlobs(String container, ListContainerOptions options) {
        return delegate().countBlobs(mapContainer(container), options);
    }

    // TODO: add other metadata later
    default void updateBlobMetadata(String containerName, String blobName, Map<String, String> userMetadata) {
        String container = mapContainer(containerName);
        BlobMetadata meta = delegate().blobMetadata(container, blobName);
        if (meta != null) {
            Map<String, String> newMetadata = new HashMap<>();
            newMetadata.putAll(meta.getUserMetadata());
            newMetadata.putAll(userMetadata);
            CopyOptions options = CopyOptions.builder()
                    .userMetadata(newMetadata)
                    .build();
            delegate().copyBlob(container, blobName, container, blobName, options);
        }
    }

    @Override
    default String copyBlob(String fromContainer, String fromName, String toContainer, String toName,
                    CopyOptions options) {
        return delegate().copyBlob(mapContainer(fromContainer), fromName, mapContainer(toContainer), toName, options);
    }

    @Override
    default MultipartUpload initiateMultipartUpload(String s, BlobMetadata blobMetadata) {
        return delegate().initiateMultipartUpload(s, blobMetadata);
    }

    @Override
    default void abortMultipartUpload(MultipartUpload multipartUpload) {
        delegate().abortMultipartUpload(multipartUpload);
    }

    @Override
    default String completeMultipartUpload(MultipartUpload multipartUpload, List<MultipartPart> list) {
        return delegate().completeMultipartUpload(multipartUpload, list);
    }

    @Override
    default MultipartPart uploadMultipartPart(MultipartUpload multipartUpload, int i, Payload payload) {
        return delegate().uploadMultipartPart(multipartUpload, i, payload);
    }

    @Override
    default List<MultipartPart> listMultipartUpload(MultipartUpload multipartUpload) {
        return delegate().listMultipartUpload(multipartUpload);
    }

    @Override
    default long getMinimumMultipartPartSize() {
        return delegate().getMinimumMultipartPartSize();
    }

    @Override
    default int getMaximumNumberOfParts() {
        return delegate().getMaximumNumberOfParts();
    }

    @Override
    default long getMaximumMultipartPartSize() {
        return delegate().getMaximumMultipartPartSize();
    }
}
