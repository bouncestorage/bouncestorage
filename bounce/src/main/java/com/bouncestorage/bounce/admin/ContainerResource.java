/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.Utils;
import com.codahale.metrics.annotation.Timed;

import org.jclouds.blobstore.domain.StorageMetadata;

@Path("/container")
@Produces(MediaType.APPLICATION_JSON)
public final class ContainerResource {
    private final BounceApplication app;

    public ContainerResource(BounceApplication app) {
        this.app = requireNonNull(app);
    }

    @GET
    @Timed
    public ContainerStats getContainerStats(
            @QueryParam("name") String containerName) {
        BounceBlobStore blobStore = app.getBlobStore();
        Iterable<StorageMetadata> metas = Utils.crawlBlobStore(blobStore,
                containerName);
        AtomicLong bounceLinkCount = new AtomicLong();
        List<String> blobNames = StreamSupport.stream(metas.spliterator(), /*parallel=*/ false)
                .map(sm -> (BounceStorageMetadata) sm)
                .peek(sm -> {
                    if (sm.getRegions().equals(BounceBlobStore.FAR_ONLY)) {
                        bounceLinkCount.getAndIncrement();
                    }
                })
                .map(sm -> sm.getName())
                .collect(Collectors.toList());
        return new ContainerStats(blobNames, bounceLinkCount.get());
    }
}
