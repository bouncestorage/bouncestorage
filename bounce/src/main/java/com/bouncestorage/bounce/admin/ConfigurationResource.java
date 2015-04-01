/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static java.util.Objects.requireNonNull;

import java.util.Properties;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;

@Path("/config")
@Produces(MediaType.APPLICATION_JSON)
public final class ConfigurationResource {
    private final BounceApplication app;

    public ConfigurationResource(BounceApplication app) {
        this.app = requireNonNull(app);
    }

    @POST
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public void updateConfig(Properties newProperties) {
        app.getConfiguration().setAll(newProperties);
    }

    @GET
    @Timed
    public Properties getConfig() {
        return app.getConfigView();
    }
}
