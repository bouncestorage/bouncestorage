/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.Utils;
import com.codahale.metrics.annotation.Timed;

import org.jclouds.blobstore.domain.StorageMetadata;

@Path("/bounce")
@Produces(MediaType.APPLICATION_JSON)
public final class BounceBlobsResource {
    private final BounceBlobStore blobStore;

    public BounceBlobsResource(BounceBlobStore blobStore) {
        this.blobStore = checkNotNull(blobStore);
    }

    @POST
    @Timed
    public void bounceBlobs(@QueryParam("name") String name)
            throws IOException {
        for (StorageMetadata sm : Utils.crawlBlobStore(blobStore, name)) {
            String blobName = sm.getName();
            if (!blobStore.isLink(name, blobName)) {
                blobStore.copyBlobAndCreateBounceLink(name, blobName);
            }
        }
    }
}
