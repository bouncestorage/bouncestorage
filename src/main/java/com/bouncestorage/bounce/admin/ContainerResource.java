/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.Utils;
import com.codahale.metrics.annotation.Timed;

@Path("/container")
@Produces(MediaType.APPLICATION_JSON)
public final class ContainerResource {
    private final BounceApplication app;

    public ContainerResource(BounceApplication app) {
        this.app = checkNotNull(app);
    }

    @GET
    @Timed
    public ContainerStats getContainerStats(
            @QueryParam("name") String containerName) {
        BounceBlobStore blobStore = app.getBlobStore();
        Iterable<Utils.ListBlobMetadata> metas = Utils.crawlBlobStore(blobStore,
                containerName);
        List<String> blobNames =
                StreamSupport.stream(metas.spliterator(), /*parallel=*/ false)
                .map(sm -> sm.metadata().getName())
                .collect(Collectors.toList());
        long bounceLinkCount = blobNames.stream()
                .filter(blobName -> blobStore.isLink(containerName, blobName))
                .count();
        return new ContainerStats(blobNames, bounceLinkCount);
    }
}
