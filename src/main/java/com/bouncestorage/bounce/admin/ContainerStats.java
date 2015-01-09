/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class ContainerStats {
    private List<String> blobNames;
    private long bounceLinkCount;

    public ContainerStats() {
        // Jackson deserialization
    }

    public ContainerStats(List<String> blobNames, long bounceLinkCount) {
        this.blobNames = blobNames;
        this.bounceLinkCount = bounceLinkCount;
    }

    @JsonProperty
    public List<String> getBlobNames() {
        return blobNames;
    }

    @JsonProperty
    public long getBounceLinkCount() {
        return bounceLinkCount;
    }
}
