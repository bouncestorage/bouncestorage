/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

import javax.annotation.Resource;

import com.bouncestorage.bounce.BounceBlobStore;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.CreationException;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.configuration.UrlConfigurationSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import org.apache.commons.configuration.AbstractConfiguration;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.ServerConnector;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.logging.Logger;


public final class BounceApplication extends Application<BounceConfiguration> {
    @Resource
    private Logger logger = Logger.NULL;

    private final AbstractConfiguration config;
    private final Properties configView;
    private BlobStoreContext blobStoreContext;
    private BounceBlobStore blobStore;
    private final List<Consumer<BlobStoreContext>> blobStoreListeners =
            new ArrayList<>();
    private final BounceService bounceService;
    private int port = -1;
    private boolean useRandomPorts;

    public BounceApplication(AbstractConfiguration config) {
        this.config = checkNotNull(config);
        this.bounceService = new BounceService(this);
        this.configView = new ConfigurationPropertiesView(config);

        config.addConfigurationListener(evt -> {
            boolean storeChanged = false;
            if (evt.getPropertyName().startsWith("bounce.store.properties.")) {
                storeChanged = true;
            }
            if (storeChanged) {
                reinitBlobStore();
            }
        });
        config.addConfigurationListener(bounceService.getConfigurationListener());
    }

    private void reinitBlobStore() {
        try {
            if (blobStoreContext != null) {
                blobStoreContext.close();
            }
            blobStoreContext = ContextBuilder
                    .newBuilder("bounce")
                    .overrides(System.getProperties())
                    .overrides(configView)
                    .build(BlobStoreContext.class);
            useBlobStore((BounceBlobStore) blobStoreContext.getBlobStore());
            blobStoreListeners.forEach(cb -> cb.accept(blobStoreContext));
        } catch (CreationException e) {
            logger.error("Unable to initialize blob: %s", e.getErrorMessages());
        }
    }

    public void useBlobStore(BounceBlobStore bounceBlobStore) {
        this.blobStore = bounceBlobStore;
    }

    public AbstractConfiguration getConfiguration() {
        return config;
    }

    public Properties getConfigView() {
        return configView;
    }

    public void addBlobStoreListener(Consumer<BlobStoreContext> listener) {
        blobStoreListeners.add(listener);
    }

    public BounceService getBounceService() {
        return bounceService;
    }

    BounceBlobStore getBlobStore() {
        return blobStore;
    }

    @VisibleForTesting
    BlobStoreContext getBlobStoreContext() {
        return blobStoreContext;
    }

    @VisibleForTesting
    void setBlobStoreContext(BlobStoreContext context) {
        if (blobStoreContext != null) {
            blobStoreContext.close();
        }
        blobStoreContext = context;
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
        environment.jersey().register(new ConfigurationResource(this));
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
        reinitBlobStore();
    }

    void useRandomPorts() {
        useRandomPorts = true;
    }

    public int getPort() {
        return port;
    }
}
