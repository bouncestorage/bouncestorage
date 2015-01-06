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
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.bouncestorage.bounce.BounceBlobStore;
import com.codahale.metrics.annotation.Timed;

import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;

@Path("/container")
@Produces(MediaType.APPLICATION_JSON)
public final class ContainerResource {
    private final BounceBlobStore blobStore;

    public ContainerResource(BounceBlobStore blobStore) {
        this.blobStore = checkNotNull(blobStore);
    }

    @GET
    @Timed
    public ContainerStats sayHello(@QueryParam("name") String containerName) {
        PageSet<? extends StorageMetadata> pageSet = blobStore.list(
                containerName);
        List<String> blobNames = pageSet.stream()
                .map(sm -> sm.getName())
                .collect(Collectors.toList());
        long bounceLinkCount = blobNames.stream()
                .filter(blobName -> blobStore.isLink(containerName, blobName))
                .count();
        return new ContainerStats(blobNames, bounceLinkCount);
    }
}
