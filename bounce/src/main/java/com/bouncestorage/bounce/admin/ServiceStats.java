/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class ServiceStats {
    private List<String> containerNames;

    public ServiceStats() {
        // Jackson deserialization
    }

    public ServiceStats(List<String> containerNames) {
        this.containerNames = containerNames;
    }

    @JsonProperty
    public List<String> getContainerNames() {
        return containerNames;
    }
}
