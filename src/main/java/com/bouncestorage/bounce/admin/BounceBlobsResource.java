/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

@Path("/bounce")
@Produces(MediaType.APPLICATION_JSON)
public final class BounceBlobsResource {
    private final BounceService service;

    public BounceBlobsResource(BounceService bounceService) {
        this.service = checkNotNull(bounceService);
    }

    @POST
    @Timed
    public BounceService.BounceTaskStatus bounceBlobs(
            @QueryParam("name") String name,
            @QueryParam("wait") Optional<Boolean> wait)
            throws ExecutionException, InterruptedException {
        BounceService.BounceTaskStatus status = service.bounce(name);
        if (wait.or(false)) {
            status.future().get();
        }
        return status;
    }

    @GET
    @Timed
    public Collection<BounceService.BounceTaskStatus> status(
            @QueryParam("name") Optional<String> name) {
        if (name.isPresent()) {
            return ImmutableSet.of(service.status(name.get()));
        } else {
            return service.status();
        }
    }
}
