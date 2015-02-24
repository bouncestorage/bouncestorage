/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Properties;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;

import org.apache.commons.configuration.Configuration;

@Path("/config")
@Produces(MediaType.APPLICATION_JSON)
public final class ConfigurationResource {
    private final BounceApplication app;

    public ConfigurationResource(BounceApplication app) {
        this.app = checkNotNull(app);
    }

    @POST
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public void updateConfig(Properties newProperties) {
        Configuration config = app.getConfiguration();
        newProperties.entrySet().forEach(
                e -> config.setProperty((String) e.getKey(), e.getValue()));
    }

    @GET
    @Timed
    public Properties getConfig() {
        return app.getConfigView();
    }
}
