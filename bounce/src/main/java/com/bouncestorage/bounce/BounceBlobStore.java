/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Resource;
import javax.inject.Inject;

import com.bouncestorage.bounce.admin.BouncePolicy;
import com.google.common.annotations.VisibleForTesting;

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
import org.jclouds.logging.Logger;
import org.jclouds.providers.ProviderMetadata;

public final class BounceBlobStore implements BlobStore {
    public static final String STORES_LIST = "bounce.backends";
    public static final String STORE_PROPERTY = "bounce.backend";
    public static final String STORE_PROPERTY_1 = STORE_PROPERTY + ".1";
    public static final String STORE_PROPERTY_2 = STORE_PROPERTY + ".2";

    @Resource
    private Logger logger = Logger.NULL;

    private BlobStoreContext context;
    private BlobStore nearStore;
    private BlobStore farStore;
    private BouncePolicy policy;

    @Inject
    BounceBlobStore(BlobStoreContext context, ProviderMetadata providerMetadata) {
        this.context = requireNonNull(context);
        Properties properties = providerMetadata.getDefaultProperties();

        initStores(Utils.extractProperties(properties, STORE_PROPERTY_1 + "."),
                Utils.extractProperties(properties, STORE_PROPERTY_2 + "."));
    }

    private void initStores(Properties prop1, Properties prop2) {
        this.nearStore = requireNonNull(Utils.storeFromProperties(requireNonNull(prop1)));
        this.farStore = Utils.storeFromProperties(requireNonNull(prop2));
    }

    BlobStore getNearStore() {
        return nearStore;
    }

    BlobStore getFarStore() {
        return farStore;
    }

    public void setPolicy(BouncePolicy newPolicy) {
        newPolicy.setBlobStores(nearStore, farStore);
        policy = newPolicy;
    }

    public BouncePolicy getPolicy() {
        return policy;
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
        return policy.listAssignableLocations();
    }

    @Override
    public PageSet<? extends StorageMetadata> list() {
        return policy.list();
    }

    @Override
    public boolean containerExists(String s) {
        return policy.containerExists(s);
    }

    @Override
    public boolean createContainerInLocation(Location location, String s) {
        return createContainerInLocation(location, s, CreateContainerOptions.NONE);
    }

    @Override
    public boolean createContainerInLocation(Location location, String s, CreateContainerOptions createContainerOptions) {
        return policy.createContainerInLocation(location, s, createContainerOptions);
    }

    @Override
    public ContainerAccess getContainerAccess(String s) {
        return policy.getContainerAccess(s);
    }

    @Override
    public void setContainerAccess(String s, ContainerAccess containerAccess) {
        policy.setContainerAccess(s, containerAccess);
    }

    @Override
    public PageSet<? extends StorageMetadata> list(String containerName) {
        return list(containerName, new ListContainerOptions().maxResults(1000));
    }

    @Override
    public PageSet<? extends StorageMetadata> list(String containerName, ListContainerOptions listContainerOptions) {
        return policy.list(containerName, listContainerOptions);
    }

    @Override
    public void clearContainer(String s) {
        clearContainer(s, ListContainerOptions.NONE);
    }

    @Override
    public void clearContainer(String s, ListContainerOptions listContainerOptions) {
        policy.clearContainer(s, listContainerOptions);
    }

    @Override
    public void deleteContainer(String s) {
        policy.deleteContainer(s);
    }

    @Override
    public boolean deleteContainerIfEmpty(String s) {
        return policy.deleteContainerIfEmpty(s);
    }

    @Override
    public boolean directoryExists(String s, String s1) {
        return policy.directoryExists(s, s1);
    }

    @Override
    public void createDirectory(String s, String s1) {
        policy.createDirectory(s, s1);
    }

    @Override
    public void deleteDirectory(String s, String s1) {
        policy.deleteDirectory(s, s1);
    }

    @Override
    public boolean blobExists(String container, String key) {
        return policy.blobExists(container, key);
    }

    @Override
    public String putBlob(String containerName, Blob blob) {
        return putBlob(containerName, blob, PutOptions.NONE);
    }

    @Override
    public String putBlob(String containerName, Blob blob, PutOptions putOptions) {
        return policy.putBlob(containerName, blob, putOptions);
    }

    public BlobMetadata blobMetadataNoFollow(String container, String s) {
        return policy.getSource().blobMetadata(container, s);
    }

    @Override
    public BlobMetadata blobMetadata(String container, String key) {
        return policy.blobMetadata(container, key);
    }

    @Override
    public Blob getBlob(String containerName, String blobName) {
        return getBlob(containerName, blobName, GetOptions.NONE);
    }

    @Override
    public Blob getBlob(String containerName, String blobName, GetOptions getOptions) {
        return policy.getBlob(containerName, blobName, getOptions);
    }

    @Override
    public void removeBlob(String s, String s1) {
        policy.removeBlob(s, s1);
    }

    @Override
    public void removeBlobs(String s, Iterable<String> iterable) {
        policy.removeBlobs(s, iterable);
    }

    @Override
    public BlobAccess getBlobAccess(String container, String name) {
        return policy.getBlobAccess(container, name);
    }

    @Override
    public void setBlobAccess(String container, String name, BlobAccess access) {
        policy.setBlobAccess(container, name, access);
    }

    @Override
    public long countBlobs(String s) {
        return countBlobs(s, ListContainerOptions.NONE);
    }

    @Override
    public long countBlobs(String container, ListContainerOptions listContainerOptions) {
        return policy.countBlobs(container, listContainerOptions);
    }

    @Override
    public MultipartUpload initiateMultipartUpload(String s, BlobMetadata blobMetadata) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abortMultipartUpload(MultipartUpload multipartUpload) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String completeMultipartUpload(MultipartUpload multipartUpload, List<MultipartPart> list) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MultipartPart uploadMultipartPart(MultipartUpload multipartUpload, int i, Payload payload) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<MultipartPart> listMultipartUpload(MultipartUpload multipartUpload) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getMinimumMultipartPartSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaximumNumberOfParts() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getMaximumMultipartPartSize() {
        throw new UnsupportedOperationException();
    }

    public void updateBlobMetadata(String containerName, String blobName, Map<String, String> userMetadata) {
        policy.updateBlobMetadata(containerName, blobName, userMetadata);
    }

    @VisibleForTesting
    public Blob getFromFarStore(String containerName, String blobName) {
        return policy.getDestination().getBlob(containerName, blobName);
    }

    @VisibleForTesting
    public Blob getFromNearStore(String containerName, String blobName) {
        return policy.getSource().getBlob(containerName, blobName);
    }

    @Override
    public String copyBlob(String fromContainer, String fromName, String toContainer, String toName, CopyOptions options) {
        return policy.copyBlob(fromContainer, fromName, toContainer, toName, options);
    }
}
