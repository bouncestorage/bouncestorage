/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.bouncestorage.bounce.BlobStoreTarget;
import com.bouncestorage.bounce.admin.BounceApplication;
import com.bouncestorage.bounce.admin.BounceConfiguration;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.bouncestorage.bounce.admin.BounceService;
import com.bouncestorage.bounce.admin.policy.WriteBackPolicy;
import com.google.common.collect.ImmutableMap;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.ContainerNotFoundException;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.ContainerAccess;
import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.blobstore.domain.internal.PageSetImpl;
import org.jclouds.blobstore.domain.internal.StorageMetadataImpl;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.domain.Location;
import org.jclouds.io.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutoConfigBlobStore implements BlobStore {
    private Map<String, BouncePolicy> policyMap;
    private BounceApplication app;

    private Logger logger = LoggerFactory.getLogger(getClass());

    public AutoConfigBlobStore(BounceApplication app) {
        policyMap = new HashMap<>();
        this.app = app;
    }

    @Override
    public String putBlob(String container, Blob blob) {
        return putBlob(container, blob, PutOptions.NONE);
    }

    @Override
    public String putBlob(String containerName, Blob blob, PutOptions options) {
        BouncePolicy policy = getPolicyFromContainer(containerName);
        String result = policy.putBlob(containerName, blob, options);
        if (result == null) {
            return null;
        }
        if (!runBounce(containerName)) {
            throw new RuntimeException("Bouncing the object failed");
        }
        return result;
    }

    @Override
    public Blob getBlob(String container, String blob) {
        return getBlob(container, blob, GetOptions.NONE);
    }

    @Override
    public Blob getBlob(String containerName, String blobName, GetOptions options) {
        BouncePolicy policy = getPolicyFromContainer(containerName);
        return policy.getBlob(containerName, blobName, options);
    }

    @Override
    public BlobMetadata blobMetadata(String container, String name) {
        BouncePolicy policy = getPolicyFromContainer(container);
        return policy.blobMetadata(container, name);
    }

    @Override
    public void removeBlob(String container, String name) {
        BouncePolicy policy = getPolicyFromContainer(container);
        policy.removeBlob(container, name);
        runBounce(container);
    }

    @Override
    public void removeBlobs(String container, Iterable<String> iterable) {
        BouncePolicy policy = getPolicyFromContainer(container);
        policy.removeBlobs(container, iterable);
    }

    @Override
    public BlobAccess getBlobAccess(String container, String blob) {
        if (!policyMap.containsKey(container)) {
            throw new ContainerNotFoundException();
        }
        return policyMap.get(container).getBlobAccess(container, blob);
    }

    @Override
    public void setBlobAccess(String container, String blob, BlobAccess blobAccess) {
        if (!policyMap.containsKey(container)) {
            throw new ContainerNotFoundException();
        }
        policyMap.get(container).setBlobAccess(container, blob, blobAccess);
    }

    @Override
    public long countBlobs(String container) {
        return countBlobs(container, ListContainerOptions.NONE);
    }

    @Override
    public long countBlobs(String container, ListContainerOptions listContainerOptions) {
        if (!policyMap.containsKey(container)) {
            throw new ContainerNotFoundException();
        }
        return policyMap.get(container).countBlobs(container, listContainerOptions);
    }

    @Override
    public MultipartUpload initiateMultipartUpload(String container, BlobMetadata blobMetadata) {
        if (!policyMap.containsKey(container)) {
            throw new ContainerNotFoundException();
        }
        return policyMap.get(container).initiateMultipartUpload(container, blobMetadata);
    }

    @Override
    public void abortMultipartUpload(MultipartUpload multipartUpload) {
        if (!policyMap.containsKey(multipartUpload.containerName())) {
            throw new ContainerNotFoundException();
        }
        policyMap.get(multipartUpload.containerName()).abortMultipartUpload(multipartUpload);
    }

    @Override
    public String completeMultipartUpload(MultipartUpload multipartUpload, List<MultipartPart> list) {
        if (!policyMap.containsKey(multipartUpload.containerName())) {
            throw new ContainerNotFoundException();
        }
        return policyMap.get(multipartUpload.containerName()).completeMultipartUpload(multipartUpload, list);
    }

    @Override
    public MultipartPart uploadMultipartPart(MultipartUpload multipartUpload, int i, Payload payload) {
        if (!policyMap.containsKey(multipartUpload.containerName())) {
            throw new ContainerNotFoundException();
        }
        return policyMap.get(multipartUpload.containerName()).uploadMultipartPart(multipartUpload, i, payload);
    }

    @Override
    public List<MultipartPart> listMultipartUpload(MultipartUpload multipartUpload) {
        if (!policyMap.containsKey(multipartUpload.containerName())) {
            throw new ContainerNotFoundException();
        }
        return policyMap.get(multipartUpload.containerName()).listMultipartUpload(multipartUpload);
    }

    @Override
    public long getMinimumMultipartPartSize() {
        return app.getBlobStore(0).getMinimumMultipartPartSize();
    }

    @Override
    public long getMaximumMultipartPartSize() {
        return app.getBlobStore(0).getMaximumMultipartPartSize();
    }

    @Override
    public int getMaximumNumberOfParts() {
        return app.getBlobStore(0).getMaximumNumberOfParts();
    }

    @Override
    public PageSet<? extends StorageMetadata> list(String container) {
        return list(container, ListContainerOptions.NONE);
    }

    @Override
    public PageSet<? extends StorageMetadata> list() {
        List<StorageMetadata> results = policyMap.keySet().stream().map(container -> {
            StorageMetadata metadata = new StorageMetadataImpl(StorageType.CONTAINER, null, container, null, null,
                    null, null, null, ImmutableMap.of(), null);
            return metadata;
        }).collect(Collectors.toList());
        return new PageSetImpl<>(results, null);
    }

    @Override
    public PageSet<? extends StorageMetadata> list(String containerName, ListContainerOptions options) {
        BouncePolicy policy = getPolicyFromContainer(containerName);
        return policy.list(containerName, options);
    }

    @Override
    public void clearContainer(String container) {
        BouncePolicy policy = getPolicyFromContainer(container);
        policy.clearContainer(container);
    }

    @Override
    public boolean containerExists(String container) {
        return policyMap.containsKey(container);
    }

    @Override
    public boolean createContainerInLocation(Location location, String container) {
        return createContainerInLocation(location, container, CreateContainerOptions.NONE);
    }

    @Override
    public boolean createContainerInLocation(Location location, String container, CreateContainerOptions options) {
        WriteBackPolicy policy = new WriteBackPolicy();
        policy.init(app, getNewConfiguration());
        BlobStore tier1 = app.getBlobStore(0);
        BlobStore tier2 = app.getBlobStore(1);
        String tier1Container = ContainerPool.getContainerPool(tier1).getContainer();
        String tier2Container = ContainerPool.getContainerPool(tier2).getContainer();
        BlobStoreTarget source = new BlobStoreTarget(tier1, tier1Container);
        BlobStoreTarget destination = new BlobStoreTarget(tier2, tier2Container);
        policy.setBlobStores(source, destination);
        policyMap.put(container, policy);
        return true;
    }

    @Override
    public ContainerAccess getContainerAccess(String container) {
        if (!policyMap.containsKey(container)) {
            throw new ContainerNotFoundException();
        }
        return policyMap.get(container).getContainerAccess(container);
    }

    @Override
    public void setContainerAccess(String container, ContainerAccess containerAccess) {
        if (!policyMap.containsKey(container)) {
            throw new ContainerNotFoundException();
        }
        policyMap.get(container).setContainerAccess(container, containerAccess);
    }

    @Override
    public void clearContainer(String container, ListContainerOptions options) {
        BouncePolicy policy = getPolicyFromContainer(container);
        policy.clearContainer(container, options);
    }

    @Override
    public void deleteContainer(String container) {
        BouncePolicy policy = getPolicyFromContainer(container);
        policy.deleteContainer(container);
        policyMap.remove(container);
    }

    @Override
    public boolean deleteContainerIfEmpty(String container) {
        BouncePolicy policy = getPolicyFromContainer(container);
        boolean result = policy.deleteContainerIfEmpty(container);
        if (result) {
            policyMap.remove(container);
        }
        return result;
    }

    @Override
    public boolean directoryExists(String container, String directory) {
        if (!policyMap.containsKey(container)) {
            throw new ContainerNotFoundException();
        }
        return policyMap.get(container).directoryExists(container, directory);
    }

    @Override
    public void createDirectory(String container, String directory) {
        if (!policyMap.containsKey(container)) {
            throw new ContainerNotFoundException();
        }
        policyMap.get(container).createDirectory(container, directory);
    }

    @Override
    public void deleteDirectory(String container, String directory) {
        if (!policyMap.containsKey(container)) {
            throw new ContainerNotFoundException();
        }
        policyMap.get(container).deleteDirectory(container, directory);
    }

    @Override
    public boolean blobExists(String container, String blob) {
        if (!policyMap.containsKey(container)) {
            throw new ContainerNotFoundException();
        }
        return policyMap.get(container).blobExists(container, blob);
    }

    @Override
    public BlobStoreContext getContext() {
        return app.getBlobStore(0).getContext();
    }

    @Override
    public BlobBuilder blobBuilder(String name) {
        return app.getBlobStore(0).blobBuilder(name);
    }

    @Override
    public Set<? extends Location> listAssignableLocations() {
        return app.getBlobStore(0).listAssignableLocations();
    }

    @Override
    public String copyBlob(String fromContainer, String fromName, String toContainer, String toName,
                           CopyOptions options) {
        if (!fromContainer.equals(toContainer)) {
            throw new IllegalArgumentException("Copy only between the same containers is supported");
        }
        BouncePolicy policy = getPolicyFromContainer(fromContainer);
        return policy.copyBlob(fromContainer, fromName, toContainer, toName, options);
    }

    public BouncePolicy getPolicyFromContainer(String containerName) {
        if (!policyMap.containsKey(containerName)) {
            throw new IllegalArgumentException(String.format("Container %s does not exist", containerName));
        }
        return policyMap.get(containerName);
    }

    private BounceConfiguration getNewConfiguration() {
        Properties properties = new Properties();
        if (System.getProperty("bounce." + WriteBackPolicy.COPY_DELAY) != null) {
            properties.setProperty(WriteBackPolicy.COPY_DELAY,
                    System.getProperty("bounce." + WriteBackPolicy.COPY_DELAY));
        } else {
            properties.setProperty(WriteBackPolicy.COPY_DELAY, "P0D");
        }
        if (System.getProperty("bounce." + WriteBackPolicy.EVICT_DELAY) != null) {
            properties.setProperty(WriteBackPolicy.EVICT_DELAY,
                    System.getProperty("bounce." + WriteBackPolicy.EVICT_DELAY));
        } else {
            properties.setProperty(WriteBackPolicy.EVICT_DELAY, "P0D");
        }
        BounceConfiguration config = new BounceConfiguration();
        config.setAll(properties);
        return config;
    }

    private boolean runBounce(String containerName) {
        BounceService service = new BounceService(app);
        BounceService.BounceTaskStatus status = service.bounce(containerName);
        try {
            status.future().get();
        } catch (InterruptedException | ExecutionException e) {
            logger.warn("Failed to run bounce on: " + containerName);
            return false;
        }
        if (status.getErrorObjectCount() != 0) {
            return false;
        }
        return true;
    }
}
