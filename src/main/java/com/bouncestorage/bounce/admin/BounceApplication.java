/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static com.google.common.base.Preconditions.checkNotNull;

import com.bouncestorage.bounce.BounceBlobStore;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.configuration.UrlConfigurationSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.ServerConnector;

public final class BounceApplication extends Application<BounceConfiguration> {
    private final ConfigurationResource config;
    private BounceBlobStore blobStore;
    private BounceService bounceService;
    private int port = -1;
    private boolean useRandomPorts;

    public BounceApplication(ConfigurationResource config) {
        this.config = checkNotNull(config);
    }

    public void useBlobStore(BounceBlobStore newBlobStore) {
        this.blobStore = checkNotNull(newBlobStore);
        this.bounceService = new BounceService(blobStore);
    }

    public BounceService getBounceService() {
        return bounceService;
    }

    BounceBlobStore getBlobStore() {
        return blobStore;
    }

    @Override
    public String getName() {
        return "bounce";
    }

    @Override
    public void initialize(Bootstrap<BounceConfiguration> bootstrap) {
        bootstrap.addBundle(new AssetsBundle());
        bootstrap.setConfigurationSourceProvider(new UrlConfigurationSourceProvider());
    }

    @Override
    public void run(BounceConfiguration configuration,
            Environment environment) {
        environment.jersey().register(new ServiceResource(this));
        environment.jersey().register(new ContainerResource(this));
        environment.jersey().register(new BounceBlobsResource(this));
        environment.jersey().register(config);
        if (useRandomPorts) {
            configuration.useRandomPorts();
        }

        environment.lifecycle().addServerLifecycleListener(server -> {
            for (Connector connector : server.getConnectors()) {
                if (connector instanceof ServerConnector) {
                    ServerConnector serverConnector = (ServerConnector) connector;
                    if ("application".equals(serverConnector.getName())) {
                        port = serverConnector.getLocalPort();
                        break;
                    }
                }
            }

            if (port == -1) {
                throw new IllegalStateException("Cannot find the application port");
            }
        });
    }

    void useRandomPorts() {
        useRandomPorts = true;
    }

    public int getPort() {
        return port;
    }
}
