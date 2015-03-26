/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;

@Path("/service")
@Produces(MediaType.APPLICATION_JSON)
public final class ServiceResource {
    private final BounceApplication app;

    public ServiceResource(BounceApplication app) {
        this.app = requireNonNull(app);
    }

    @GET
    @Timed
    public ServiceStats getServiceStats() {
        BlobStore store = app.getBlobStore();
        if (store == null) {
            return new ServiceStats(new ArrayList<>());
        }

        PageSet<? extends StorageMetadata> pageSet = app.getBlobStore().list();
        List<String> containerNames = pageSet.stream()
                .map(sm -> sm.getName())
                .collect(Collectors.toList());
        return new ServiceStats(containerNames);
    }
}
