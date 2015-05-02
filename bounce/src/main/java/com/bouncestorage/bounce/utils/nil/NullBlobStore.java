/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.utils.nil;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Throwables.propagate;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;

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
import org.jclouds.blobstore.domain.internal.BlobMetadataImpl;
import org.jclouds.blobstore.domain.internal.PageSetImpl;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.blobstore.util.BlobUtils;
import org.jclouds.domain.Location;
import org.jclouds.io.MutableContentMetadata;
import org.jclouds.io.Payload;
import org.jclouds.io.payloads.BaseMutableContentMetadata;

public final class NullBlobStore implements BlobStore {
    private final BlobStoreContext context;
    private final BlobUtils blobUtils;

    @Inject
    NullBlobStore(BlobStoreContext context, BlobUtils blobUtils) {
        this.context = requireNonNull(context, "context");
        this.blobUtils = requireNonNull(blobUtils, "blobUtils");
    }

    @Override
    public BlobStoreContext getContext() {
        return context;
    }

    @Override
    public BlobBuilder blobBuilder(String name) {
        return blobUtils.blobBuilder().name(name);
    }

    @Override
    public Set<? extends Location> listAssignableLocations() {
        return null;
    }

    @Override
    public PageSet<? extends StorageMetadata> list() {
        return new PageSetImpl<>(ImmutableList.of(), null);
    }

    @Override
    public boolean containerExists(String container) {
        return true;
    }

    @Override
    public boolean createContainerInLocation(Location location, String container) {
        return createContainerInLocation(location, container, CreateContainerOptions.NONE);
    }

    @Override
    public boolean createContainerInLocation(Location location, String container, CreateContainerOptions options) {
        return true;
    }

    @Override
    public ContainerAccess getContainerAccess(String container) {
        return ContainerAccess.PUBLIC_READ;
    }

    @Override
    public void setContainerAccess(String container, ContainerAccess access) {
    }

    @Override
    public PageSet<? extends StorageMetadata> list(String container) {
        return list(container, ListContainerOptions.NONE);
    }

    @Override
    public PageSet<? extends StorageMetadata> list(String container, ListContainerOptions options) {
        return new PageSetImpl<>(ImmutableList.of(), null);
    }

    @Override
    public void clearContainer(String container) {
        clearContainer(container, ListContainerOptions.NONE);
    }

    @Override
    public void clearContainer(String container, ListContainerOptions options) {
    }

    @Override
    public void deleteContainer(String container) {
    }

    @Override
    public boolean deleteContainerIfEmpty(String container) {
        return true;
    }

    @Override
    public boolean directoryExists(String container, String directory) {
        return true;
    }

    @Override
    public void createDirectory(String container, String directory) {
    }

    @Override
    public void deleteDirectory(String containerName, String name) {
    }

    @Override
    public boolean blobExists(String container, String name) {
        return true;
    }

    @Override
    public String putBlob(String container, Blob blob) {
        return putBlob(container, blob, PutOptions.NONE);
    }

    @Override
    public String putBlob(String container, Blob blob, PutOptions options) {
        try (InputStream in = blob.getPayload().openStream()) {
            ByteStreams.copy(in, ByteStreams.nullOutputStream());
        } catch (IOException e) {
            throw propagate(e);
        }
        return "";
    }

    @Override
    public String copyBlob(String fromContainer, String fromName, String toContainer, String toName, CopyOptions options) {
        return "";
    }

    @Override
    public BlobMetadata blobMetadata(String container, String name) {
        Date now = Date.from(Instant.now());
        MutableContentMetadata meta = new BaseMutableContentMetadata();
        meta.setContentLength(0L);
        String mode = Integer.valueOf(0100777).toString();
        return new BlobMetadataImpl(context.unwrap().getId(), name, null, null, "",
                now, now, ImmutableMap.of("mode", mode), null, container, meta);
    }

    @Override
    public Blob getBlob(String container, String name) {
        return getBlob(container, name, GetOptions.NONE);
    }

    @Override
    public Blob getBlob(String container, String name, GetOptions options) {
        return null;
    }

    @Override
    public void removeBlob(String container, String name) {
    }

    @Override
    public void removeBlobs(String container, Iterable<String> names) {
    }

    @Override
    public BlobAccess getBlobAccess(String container, String name) {
        return BlobAccess.PUBLIC_READ;
    }

    @Override
    public void setBlobAccess(String container, String name, BlobAccess access) {
    }

    @Override
    public long countBlobs(String container) {
        return 0;
    }

    @Override
    public long countBlobs(String container, ListContainerOptions options) {
        return 0;
    }

    @Override
    public MultipartUpload initiateMultipartUpload(String s, BlobMetadata blobMetadata) {
        return new MultipartUpload() {
            @Override
            public String containerName() {
                return null;
            }

            @Override
            public String blobName() {
                return null;
            }

            @Override
            public String id() {
                return null;
            }

            @Override
            public BlobMetadata blobMetadata() {
                return null;
            }
        };
    }

    @Override
    public void abortMultipartUpload(MultipartUpload multipartUpload) {

    }

    @Override
    public String completeMultipartUpload(MultipartUpload multipartUpload, List<MultipartPart> list) {
        return null;
    }

    @Override
    public MultipartPart uploadMultipartPart(MultipartUpload multipartUpload, int i, Payload payload) {
        return MultipartPart.create(i, -1, "");
    }

    @Override
    public List<MultipartPart> listMultipartUpload(MultipartUpload multipartUpload) {
        return ImmutableList.<MultipartPart>of();
    }

    @Override
    public long getMinimumMultipartPartSize() {
        return 1;
    }

    @Override
    public int getMaximumNumberOfParts() {
        return Integer.MAX_VALUE;
    }

    @Override
    public long getMaximumMultipartPartSize() {
        return Long.MAX_VALUE;
    }
}
