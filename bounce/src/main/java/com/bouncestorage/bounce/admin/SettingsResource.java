/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.util.Properties;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.bouncestorage.bounce.utils.KeyStoreUtils;
import com.bouncestorage.swiftproxy.SwiftProxy;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;

import org.apache.commons.configuration.Configuration;
import org.bouncycastle.operator.OperatorCreationException;
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
    public Settings getSettings() throws URISyntaxException, GeneralSecurityException,
            IOException, OperatorCreationException {
        Settings res = new Settings();
        res.s3Domain = app.getConfiguration().getString(S3ProxyConstants.PROPERTY_VIRTUAL_HOST);
        if (!Strings.isNullOrEmpty(res.s3Domain)) {
            KeyStoreUtils keystore = app.getKeyStoreUtils();
            if (keystore != null) {
                X509Certificate cert = keystore.ensureCertificate("*." + res.s3Domain);
                res.domainCertificate = keystore.exportToPem(cert);
            }
        }

        String s3URL = app.getConfiguration().getString(S3ProxyConstants.PROPERTY_ENDPOINT);
        if (s3URL != null) {
            URI endpoint = new URI(s3URL);
            res.s3Address = endpoint.getHost();
            res.s3Port = endpoint.getPort();
        }
        String s3SSLURL = app.getConfiguration().getString(S3ProxyConstants.PROPERTY_SECURE_ENDPOINT);
        if (s3SSLURL != null) {
            URI endpoint = new URI(s3SSLURL);
            res.s3SSLAddress = endpoint.getHost();
            res.s3SSLPort = endpoint.getPort();
        }
        String swiftURL = app.getConfiguration().getString(SwiftProxy.PROPERTY_ENDPOINT);
        if (swiftURL != null) {
            URI swiftEndpoint = new URI(swiftURL);
            res.swiftAddress = swiftEndpoint.getHost();
            res.swiftPort = swiftEndpoint.getPort();
        }
        return res;
    }

    private void updateSetting(Configuration config, Properties p, String key, Object value) {
        if (config.getString(key) != null && value == null) {
            p.setProperty(key, "");
        } else if (value != null) {
            p.setProperty(key, value.toString());
        }
    }

    @POST
    @Timed
    public Response updateSettings(Settings settings) throws URISyntaxException {
        if (!settings.validate()) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        Properties p = new Properties();
        URI s3Endpoint = settings.getEndpoint(Settings.Proxy.S3Proxy);
        URI s3SSLEndpoint = settings.getEndpoint(Settings.Proxy.S3SSLProxy);
        URI swiftEndpoint = settings.getEndpoint(Settings.Proxy.SwiftProxy);
        BounceConfiguration config = app.getConfiguration();

        updateSetting(config, p, S3ProxyConstants.PROPERTY_ENDPOINT, s3Endpoint);
        updateSetting(config, p, S3ProxyConstants.PROPERTY_SECURE_ENDPOINT, s3SSLEndpoint);
        updateSetting(config, p, SwiftProxy.PROPERTY_ENDPOINT, swiftEndpoint);
        updateSetting(config, p, S3ProxyConstants.PROPERTY_VIRTUAL_HOST, settings.s3Domain);

        try {
            app.getConfiguration().setAll(p);
        } catch (Throwable e) {
            if (e.getCause() != null) {
                e = e.getCause();
            }
            return Response.serverError().entity(e).build();
        }
        return Response.ok().build();
    }

    static class Settings {
        enum Proxy { S3Proxy, S3SSLProxy, SwiftProxy };
        @JsonProperty
        String s3Domain;
        @JsonProperty
        String s3Address;
        @JsonProperty
        int s3Port = -1;
        @JsonProperty
        String s3SSLAddress;
        @JsonProperty
        int s3SSLPort = -1;
        @JsonProperty
        String swiftAddress;
        @JsonProperty
        int swiftPort = -1;
        @JsonProperty
        String domainCertificate;

        URI getEndpoint(Proxy type) throws URISyntaxException {
            String address;
            int port;
            String scheme = "http";
            switch (type) {
                case S3Proxy:
                    address = s3Address;
                    port = s3Port;
                    break;
                case S3SSLProxy:
                    scheme = "https";
                    address = s3SSLAddress;
                    port = s3SSLPort;
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
            return new URI(scheme, null, address, port, null, null, null);
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
