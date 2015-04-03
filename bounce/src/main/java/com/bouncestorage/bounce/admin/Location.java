/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.commons.lang.builder.HashCodeBuilder;

public final class Location {
    public static final String BLOB_STORE_ID_FIELD = "backend";
    public static final String CONTAINER_NAME_FIELD = "container";

    private int blobStoreId = -1;
    private String containerName;

    public void setBlobStoreId(int id) {
        blobStoreId = id;
    }

    public int getBlobStoreId() {
        return blobStoreId;
    }

    public void setContainerName(String name) {
        containerName = name;
    }

    public String getContainerName() {
        return containerName;
    }

    public void setLocation(Location location) {
        containerName = location.containerName;
        blobStoreId = location.blobStoreId;
    }

    @Override
    public String toString() {
        return "blobStoreId: " + blobStoreId + " container: " + containerName;
    }

    @Override
    public boolean equals(Object other) {
        return other != null && other.getClass() == Location.class
                && containerName.equals(((Location) other).getContainerName())
                && blobStoreId == ((Location) other).getBlobStoreId();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(5, 127).append(containerName).append(blobStoreId).toHashCode();
    }

    @JsonIgnore
    public boolean isUnset() {
        return containerName == null || blobStoreId < 0;
    }

    public void setField(String key, String value) {
        if (key.equals(BLOB_STORE_ID_FIELD)) {
            setBlobStoreId(Integer.parseInt(value));
        } else if (key.equals(CONTAINER_NAME_FIELD)) {
            setContainerName(value);
        }
    }
}
