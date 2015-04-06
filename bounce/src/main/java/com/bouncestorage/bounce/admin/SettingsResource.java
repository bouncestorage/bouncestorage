/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static java.util.Objects.requireNonNull;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.gaul.s3proxy.S3ProxyConstants;

@Path("/settings")
@Produces(MediaType.APPLICATION_JSON)
public class SettingsResource {
    private final BounceApplication app;

    public SettingsResource(BounceApplication app) {
        this.app = requireNonNull(app);
    }

    @GET
    @Timed
    public Settings getSettings() throws URISyntaxException {
        Settings res = new Settings();
        URI endpoint = new URI(app.getConfiguration().getString(
                S3ProxyConstants.PROPERTY_ENDPOINT));
        res.s3Address = endpoint.getHost();
        res.s3Port = endpoint.getPort();
        return res;
    }

    @POST
    @Timed
    public void updateSettings(Settings settings) throws URISyntaxException {
        Properties p = new Properties();
        URI endpoint = new URI("http", null, settings.s3Address, settings.s3Port, null, null, null);
        p.put(S3ProxyConstants.PROPERTY_ENDPOINT, endpoint.toString());
        app.getConfiguration().setAll(p);
    }

    private static class Settings {
        @JsonProperty
        private String s3Address;
        @JsonProperty
        private int s3Port;
    }
}
