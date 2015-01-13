/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.bouncestorage.bounce.BounceBlobStore;
import com.codahale.metrics.annotation.Timed;

import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;

@Path("/service")
@Produces(MediaType.APPLICATION_JSON)
public final class ServiceResource {
    private final BounceBlobStore blobStore;

    public ServiceResource(BounceBlobStore blobStore) {
        this.blobStore = checkNotNull(blobStore);
    }

    @GET
    @Timed
    public ServiceStats getServiceStats() {
        PageSet<? extends StorageMetadata> pageSet = blobStore.list();
        List<String> containerNames = pageSet.stream()
                .map(sm -> sm.getName())
                .collect(Collectors.toList());
        return new ServiceStats(containerNames);
    }
}