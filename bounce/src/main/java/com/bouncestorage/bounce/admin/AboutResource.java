/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static com.google.common.base.Throwables.propagate;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;

@Path("/about")
@Produces(MediaType.APPLICATION_JSON)
public class AboutResource {
    private static final Properties properties;

    /** Use a static initializer to read from file. */
    static {
        properties = new Properties();
        try (InputStream inputStream = AboutResource.class.getResourceAsStream("/bounce-build.properties")) {
            properties.load(inputStream);
        } catch (IOException e) {
            throw propagate(e);
        }
    }

    @GET
    @Path("build")
    @Timed
    public Properties getBuildProperties() {
        return properties;
    }
}
