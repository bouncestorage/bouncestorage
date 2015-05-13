/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import static java.util.Objects.requireNonNull;

import java.net.URI;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ForwardingObject;
import com.google.common.collect.ImmutableSet;

import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.domain.Location;
import org.jclouds.domain.ResourceMetadata;

public final class BounceStorageMetadata extends ForwardingObject implements StorageMetadata {
    public enum Region {
        NEAR,
        FAR,
        FARTHER
    }

    public static final ImmutableSet<Region> FAR_ONLY = ImmutableSet.of(Region.FAR);
    public static final ImmutableSet<Region> NEAR_ONLY = ImmutableSet.of(Region.NEAR);
    public static final ImmutableSet<Region> EVERYWHERE = ImmutableSet.of(Region.NEAR, Region.FAR);
    private final StorageMetadata delegate;
    private final ImmutableSet<Region> regions;
    private boolean hasMarkerBlob;

    public BounceStorageMetadata(StorageMetadata metadata, Set<Region> regions) {
        this.delegate = requireNonNull(metadata);
        this.regions = ImmutableSet.copyOf(requireNonNull(regions));
    }

    public void hasMarkerBlob(boolean b) {
        hasMarkerBlob = b;
    }

    public boolean hasMarkerBlob() {
        return hasMarkerBlob;
    }

    public ImmutableSet<Region> getRegions() {
        return regions;
    }

    @Override
    protected Object delegate() {
        return delegate;
    }

    @Override
    public StorageType getType() {
        return delegate.getType();
    }

    @Override
    public String getProviderId() {
        return delegate.getProviderId();
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public Location getLocation() {
        return delegate.getLocation();
    }

    @Override
    public URI getUri() {
        return delegate.getUri();
    }

    @Override
    public Map<String, String> getUserMetadata() {
        return delegate.getUserMetadata();
    }

    @Override
    public String getETag() {
        return delegate.getETag();
    }

    @Override
    public Date getCreationDate() {
        return delegate.getCreationDate();
    }

    @Override
    public Date getLastModified() {
        return delegate.getLastModified();
    }

    @Override
    public Long getSize() {
        return delegate.getSize();
    }

    @Override
    public int compareTo(ResourceMetadata<StorageType> o) {
        return delegate.compareTo(o);
    }
}
