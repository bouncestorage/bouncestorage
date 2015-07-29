/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import static java.util.Objects.requireNonNull;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.internal.MutableStorageMetadataImpl;

public final class BounceStorageMetadata extends MutableStorageMetadataImpl {
    public enum Region {
        NEAR,
        FAR,
        FARTHER
    }

    public static final ImmutableSet<Region> FAR_ONLY = ImmutableSet.of(Region.FAR);
    public static final ImmutableSet<Region> NEAR_ONLY = ImmutableSet.of(Region.NEAR);
    public static final ImmutableSet<Region> EVERYWHERE = ImmutableSet.of(Region.NEAR, Region.FAR);
    private final ImmutableSet<Region> regions;
    private boolean hasMarkerBlob;

    public BounceStorageMetadata(StorageMetadata metadata, Set<Region> regions) {
        super(metadata);
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
}
