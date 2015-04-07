/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import com.bouncestorage.bounce.admin.policy.WriteBackPolicy;
import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.commons.lang.builder.HashCodeBuilder;

public final class Location {
    public static final String BLOB_STORE_ID_FIELD = "backend";
    public static final String CONTAINER_NAME_FIELD = "container";

    private int blobStoreId = -1;
    private String containerName;
    private String copyDelay;
    private String moveDelay;

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
        copyDelay = location.copyDelay;
        moveDelay = location.moveDelay;
    }

    public String getCopyDelay() {
        return copyDelay;
    }

    public void setCopyDelay(String copyDelay) {
        this.copyDelay = copyDelay;
    }

    public String getMoveDelay() {
        return moveDelay;
    }

    public void setMoveDelay(String moveDelay) {
        this.moveDelay = moveDelay;
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
        } else if (key.equals(WriteBackPolicy.COPY_DELAY)) {
            setCopyDelay(value);
        } else if (key.equals(WriteBackPolicy.EVICT_DELAY)) {
            setMoveDelay(value);
        }
    }

    public boolean permittedChange(Location other) {
        if (isUnset()) {
            return true;
        }

        if (blobStoreId != other.getBlobStoreId()) {
            return false;
        }
        if (!containerName.equals(other.getContainerName())) {
            return false;
        }

        return true;
    }
}
