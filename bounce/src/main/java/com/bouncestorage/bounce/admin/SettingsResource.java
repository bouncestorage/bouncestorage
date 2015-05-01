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
import javax.ws.rs.core.Response;

import com.bouncestorage.swiftproxy.SwiftProxy;
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
        URI swiftEndpoint = new URI(app.getConfiguration().getString(
                SwiftProxy.PROPERTY_ENDPOINT));
        res.swiftAddress = swiftEndpoint.getHost();
        res.swiftPort = swiftEndpoint.getPort();
        return res;
    }

    @POST
    @Timed
    public Response updateSettings(Settings settings) throws URISyntaxException {
        if (!settings.validate()) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        Properties p = new Properties();
        URI s3Endpoint = settings.getEndpoint(Settings.PROXY.S3Proxy);
        URI swiftEndpoint = settings.getEndpoint(Settings.PROXY.SwiftProxy);
        BounceConfiguration config = app.getConfiguration();
        if (config.getString(S3ProxyConstants.PROPERTY_ENDPOINT) != null && s3Endpoint == null) {
            p.setProperty(S3ProxyConstants.PROPERTY_ENDPOINT, "");
        } else if (s3Endpoint != null) {
            p.put(S3ProxyConstants.PROPERTY_ENDPOINT, s3Endpoint.toString());
        }

        if (config.getString(SwiftProxy.PROPERTY_ENDPOINT) != null && swiftEndpoint == null) {
            p.setProperty(SwiftProxy.PROPERTY_ENDPOINT, "");
        } else if (swiftEndpoint != null) {
            p.put(SwiftProxy.PROPERTY_ENDPOINT, swiftEndpoint.toString());
        }
        app.getConfiguration().setAll(p);
        return Response.ok().build();
    }

    private static class Settings {
        enum PROXY { S3Proxy, SwiftProxy };
        @JsonProperty
        private String s3Address;
        @JsonProperty
        private int s3Port = -1;
        @JsonProperty
        private String swiftAddress;
        @JsonProperty
        private int swiftPort = -1;

        private URI getEndpoint(PROXY type) throws URISyntaxException {
            String address;
            int port;
            switch (type) {
                case S3Proxy:
                    address = s3Address;
                    port = s3Port;
                    break;
                case SwiftProxy:
                    address = swiftAddress;
                    port = swiftPort;
                    break;
                default:
                    address = null;
                    port = -1;
            }
            if (address == null || port < 0) {
                return null;
            }
            return new URI("http", null, address, port, null, null, null);
        }

        private boolean validate() {
            if (swiftAddress == null) {
                return true;
            }
            if (s3Address == null) {
                return true;
            }
            return !(swiftAddress.equalsIgnoreCase(s3Address) && swiftPort == s3Port);
        }
    }
}
