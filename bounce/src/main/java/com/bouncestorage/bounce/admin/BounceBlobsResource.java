/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;

@Path("/bounce")
@Produces(MediaType.APPLICATION_JSON)
public final class BounceBlobsResource {
    private final BounceApplication app;

    public BounceBlobsResource(BounceApplication app) {
        this.app = requireNonNull(app);
    }

    @POST
    @Path("{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed
    public BounceService.BounceTaskStatus bounceBlobs(
            BounceServiceRequest request)
            throws ExecutionException, InterruptedException {
        BounceService service = app.getBounceService();
        BounceService.BounceTaskStatus status = request.abort ? service.status(request.name) :
                service.bounce(request.name);
        if (status == null) {
            return null;
        }
        if (request.abort) {
            status.abort();
        }
        if (request.wait) {
            status.future().get();
        }
        return status;
    }

    @GET
    @Path("{name}")
    @Timed
    public BounceService.BounceTaskStatus status(@PathParam("name") String name) {
        BounceService service = app.getBounceService();
        return service.status(name);
    }

    @GET
    @Timed
    public Collection<BounceService.BounceTaskStatus> status() {
        BounceService service = app.getBounceService();
        return service.status();
    }

    public static class BounceServiceRequest {
        private String name;
        private Boolean wait;
        private Boolean abort;

        public BounceServiceRequest() {
            this(Optional.absent(), Optional.absent(), Optional.absent());
        }

        public BounceServiceRequest(Optional<String> name, Optional<Boolean> wait, Optional<Boolean> abort) {
            this.wait = wait.or(false);
            this.abort = abort.or(false);
            this.name = name.or("");
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Boolean isWait() {
            return wait;
        }

        public void setWait(Boolean wait) {
            this.wait = wait;
        }

        public Boolean isAbort() {
            return abort;
        }

        public void setAbort(Boolean abort) {
            this.abort = abort;
        }
    }
}
