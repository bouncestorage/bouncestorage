/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static java.util.Objects.requireNonNull;

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
    private final BounceApplication app;

    public BounceBlobsResource(BounceApplication app) {
        this.app = requireNonNull(app);
    }

    @POST
    @Timed
    public BounceService.BounceTaskStatus bounceBlobs(
            @QueryParam("name") String name,
            @QueryParam("wait") Optional<Boolean> wait,
            @QueryParam("abort") Optional<Boolean> abortParam)
            throws ExecutionException, InterruptedException {
        BounceService service = app.getBounceService();
        boolean abort = abortParam.or(false);
        BounceService.BounceTaskStatus status = abort ? service.status(name) : service.bounce(name);
        if (status == null) {
            return null;
        }
        if (abort) {
            status.abort();
        }
        if (wait.or(false)) {
            status.future().get();
        }
        return status;
    }

    @GET
    @Timed
    public Collection<BounceService.BounceTaskStatus> status(
            @QueryParam("name") Optional<String> name) {
        BounceService service = app.getBounceService();
        if (name.isPresent()) {
            return ImmutableSet.of(service.status(name.get()));
        } else {
            return service.status();
        }
    }
}
