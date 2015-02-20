/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

import ch.qos.logback.classic.Level;
import com.bouncestorage.bounce.BounceBlobStore;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.CreationException;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.configuration.UrlConfigurationSourceProvider;
import io.dropwizard.logging.LoggingFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import org.apache.commons.configuration.AbstractConfiguration;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.ServerConnector;
import org.gaul.s3proxy.S3Proxy;
import org.gaul.s3proxy.S3ProxyConstants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class BounceApplication extends Application<BounceConfiguration> {
    private static final ImmutableSet<String> REQUIRED_PROPERTIES =
            ImmutableSet.of(S3ProxyConstants.PROPERTY_ENDPOINT,
                    S3ProxyConstants.PROPERTY_IDENTITY,
                    S3ProxyConstants.PROPERTY_CREDENTIAL);

    private Logger logger = LoggerFactory.getLogger(getClass());

    private final AbstractConfiguration config;
    private final Properties configView;
    private BounceBlobStore blobStore;
    private final List<Consumer<BlobStoreContext>> blobStoreListeners =
            new ArrayList<>();
    private final BounceService bounceService;
    private int port = -1;
    private boolean useRandomPorts;
    private S3Proxy s3Proxy;

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

        addBlobStoreListener(context -> {
            try {
                if (s3Proxy != null) {
                    s3Proxy.stop();
                }
                S3Proxy.Builder builder = S3Proxy.builder()
                        .blobStore(context.getBlobStore())
                        .endpoint(new URI(config.getString(
                                S3ProxyConstants.PROPERTY_ENDPOINT)))
                        .forceMultiPartUpload(
                                "true".equalsIgnoreCase((String) configView.get(
                                        S3ProxyConstants.PROPERTY_FORCE_MULTI_PART_UPLOAD)));

                String identity = (String) configView.get(
                        S3ProxyConstants.PROPERTY_IDENTITY);
                String credential = (String) configView.get(
                        S3ProxyConstants.PROPERTY_CREDENTIAL);
                if (identity != null || credential != null) {
                    builder.awsAuthentication(identity, credential);
                }

                String keyStorePath = (String) configView.get(
                        S3ProxyConstants.PROPERTY_KEYSTORE_PATH);
                String keyStorePassword = (String) configView.get(
                        S3ProxyConstants.PROPERTY_KEYSTORE_PASSWORD);
                if (keyStorePath != null || keyStorePassword != null) {
                    builder.keyStore(keyStorePath, keyStorePassword);
                }

                String virtualHost = config.getString(
                        S3ProxyConstants.PROPERTY_VIRTUAL_HOST);
                if (virtualHost != null) {
                    builder.virtualHost(virtualHost);
                }

                s3Proxy = builder.build();
                s3Proxy.start();
            } catch (Exception e) {
                throw propagate(e);
            }
        });
        config.addConfigurationListener(bounceService.getConfigurationListener());
    }

    private boolean isConfigValid() {
        return configView.stringPropertyNames().containsAll(
                REQUIRED_PROPERTIES);
    }

    private void reinitBlobStore() {
        if (!isConfigValid()) {
            logger.error("Missing parameters: {}.", Sets.difference(REQUIRED_PROPERTIES,
                    configView.stringPropertyNames()));
            return;
        }
        try {
            BlobStoreContext context = ContextBuilder
                    .newBuilder("bounce")
                    .overrides(System.getProperties())
                    .overrides(configView)
                    .build(BlobStoreContext.class);
            useBlobStore((BounceBlobStore) context.getBlobStore());
            blobStoreListeners.forEach(cb -> cb.accept(context));
        } catch (CreationException e) {
            logger.error("Unable to initialize blob: {}", e.getErrorMessages());
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

    public String getS3ProxyState() {
        checkNotNull(s3Proxy);
        return s3Proxy.getState();
    }

    public int getS3ProxyPort() {
        return s3Proxy.getPort();
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
    public void run(BounceConfiguration configuration, Environment environment) {
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

    public void stop() throws Exception {
        if (s3Proxy != null) {
            s3Proxy.stop();
        }
    }

    void useRandomPorts() {
        useRandomPorts = true;
    }

    public int getPort() {
        return port;
    }

    static {
        // DropWizard's Application class has a static initializer that forces the filter
        // to be at WARN, this overrides that
        LoggingFactory.bootstrap(Level.toLevel(System.getProperty("LOG_LEVEL"), Level.INFO));
    }
}
