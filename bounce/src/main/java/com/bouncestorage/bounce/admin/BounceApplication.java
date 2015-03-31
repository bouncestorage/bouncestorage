/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Throwables.propagate;

import java.net.URI;
import java.time.Clock;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.CreationException;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.configuration.UrlConfigurationSourceProvider;
import io.dropwizard.logging.LoggingFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.Configuration;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.ServerConnector;
import org.gaul.s3proxy.S3Proxy;
import org.gaul.s3proxy.S3ProxyConstants;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;


public final class BounceApplication extends Application<BounceConfiguration> {
    private static final ImmutableSet<String> REQUIRED_PROPERTIES =
            ImmutableSet.of(S3ProxyConstants.PROPERTY_ENDPOINT);

    private Logger logger = LoggerFactory.getLogger(getClass());

    private final AbstractConfiguration config;
    private final Properties configView;

    private BounceService bounceService;
    private int port = -1;
    private boolean useRandomPorts;
    private S3Proxy s3Proxy;
    private Map<Integer, BlobStore> providers = new HashMap<>();
    private Map<String, BouncePolicy> virtualContainers = new HashMap<>();
    private final Pattern providerConfigPattern = Pattern.compile("(bounce.backend.\\d+).jclouds.provider");
    private final Pattern containerConfigPattern = Pattern.compile("(bounce.container.\\d+).name");
    private Clock clock = Clock.systemUTC();

    public BounceApplication(AbstractConfiguration config) {
        this.config = requireNonNull(config);
        this.configView = new ConfigurationPropertiesView(config);

        config.addConfigurationListener(evt -> {
            String name = evt.getPropertyName();
            Matcher m;

            if (!evt.isBeforeUpdate()) {
                if ((m = providerConfigPattern.matcher(name)).matches()) {
                    addProviderFromConfig(m.group(1), (String) evt.getPropertyValue());
                } else if ((m = containerConfigPattern.matcher(name)).matches()) {
                    addContainerFromConfig(m.group(1), (String) evt.getPropertyValue());
                }
            }
        });
    }

    private void startS3Proxy() {
        try {
            if (s3Proxy != null) {
                s3Proxy.stop();
            }
            S3Proxy.Builder builder = S3Proxy.builder()
                    .endpoint(new URI(config.getString(
                            S3ProxyConstants.PROPERTY_ENDPOINT)));

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
            s3Proxy.setBlobStoreLocator((i, c, b) -> locateBlobStore(i, c, b));
            s3Proxy.start();
        } catch (Exception e) {
            throw propagate(e);
        }
    }

    private void addProviderFromConfig(String prefix, String provider) {
        logger.info("adding provider from {}", prefix);
        Configuration c = config.subset(prefix);
        BlobStoreContext context;
        try {
            ContextBuilder builder = ContextBuilder.newBuilder(provider);
            String identity = c.getString(Constants.PROPERTY_IDENTITY);
            if (identity != null) {
                builder.credentials(identity, c.getString(Constants.PROPERTY_CREDENTIAL));
            }
            context = builder.overrides(new ConfigurationPropertiesView(c))
                    .build(BlobStoreContext.class);
        } catch (CreationException e) {
            e.printStackTrace();
            throw propagate(e);
        }

        addProvider(context.getBlobStore());
    }

    static Optional<BouncePolicy> getBouncePolicyFromName(String name) {
        ServiceLoader<BouncePolicy> loader = ServiceLoader.load(BouncePolicy.class);
        return StreamSupport.stream(loader.spliterator(), false)
                .filter(p -> p.getClass().getSimpleName().equals(name))
                .findAny();
    }

    private void addContainerFromConfig(String prefix, String containerName) {
        logger.info("adding container from {}: {}", prefix);
        Configuration c = config.subset(prefix);
        int sourceId = c.getInt("tier.0.backend");
        int destId = c.getInt("tier.1.backend");
        BlobStore source = providers.get(sourceId);
        BlobStore dest = providers.get(destId);
        Optional<BouncePolicy> policy = getBouncePolicyFromName(c.getString("tier.0.policy"));
        policy.ifPresent(p -> {
            p.init(this, c.subset("tier.0"));
            p.setBlobStores(source, dest);
            virtualContainers.put(containerName, p);
        });
    }

    Collection<BouncePolicy> getPolicies() {
        return virtualContainers.values();
    }

    public BlobStore getBlobStore() {
        if (providers.isEmpty()) {
            return null;
        }

        return providers.values().iterator().next();
    }

    public BlobStore getBlobStore(String containerName) {
        if (providers.isEmpty()) {
            return null;
        }
        BlobStore blobStore = virtualContainers.get(containerName);
        if (blobStore == null) {
            blobStore = providers.values().iterator().next();
        }

        return blobStore;
    }

    private Optional<String> getCredentialOfContainer(String container) {
        // TODO implement
        return Optional.empty();
    }

    public void addProvider(BlobStore blobStore) {
        int nextID = providers.keySet().stream().reduce(Math::max).orElse(-1) + 1;
        logger.info("allocated provider id: {}", nextID);
        providers.put(nextID, blobStore);
    }

    @VisibleForTesting
    public Map.Entry<String, BlobStore> locateBlobStore(String identity,
                                                        String container, String blob) {
        BlobStore blobStore = null;
        Optional<String> credential = Optional.empty();

        if (container != null) {
            blobStore = virtualContainers.get(container);

            if (blobStore != null) {
                credential = getCredentialOfContainer(container);
            }
        }

        if (blobStore == null) {
            List<Object> backendIDs = config.getList("bounce.backends");
            logger.info("keys: {}", backendIDs);
            for (Object id : backendIDs) {
                Configuration c = config.subset("bounce.backend." + id);
                if (identity.equals(c.getString(Constants.PROPERTY_IDENTITY))) {
                    credential = Optional.of(c.getString(Constants.PROPERTY_CREDENTIAL));
                    blobStore = providers.get(Integer.valueOf(id.toString()));
                }
            }
        }

        if (blobStore != null && credential.isPresent()) {
            logger.info("identity: {} credential: {}", identity, credential);
            return Maps.immutableEntry(credential.get(), blobStore);
        }

        return null;
    }

    private boolean isConfigValid() {
        return configView.stringPropertyNames().containsAll(
                REQUIRED_PROPERTIES);
    }

    private void initFromConfig() {
        if (!isConfigValid()) {
            logger.error("Missing parameters: {}.", Sets.difference(REQUIRED_PROPERTIES,
                    configView.stringPropertyNames()));
            return;
        }

        config.getList("bounce.backends").forEach(id ->
                addProviderFromConfig("bounce.backend." + id,
                        config.getString("bounce.backend." + id + ".jclouds.provider")));
        config.getList("bounce.containers").forEach(id ->
                addContainerFromConfig("bounce.container." + id,
                        config.getString("bounce.container." + id + ".name")));
    }

    public AbstractConfiguration getConfiguration() {
        return config;
    }

    public Properties getConfigView() {
        return configView;
    }

    public BounceService getBounceService() {
        return bounceService;
    }

    public String getS3ProxyState() {
        requireNonNull(s3Proxy);
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

        startS3Proxy();
        bounceService = new BounceService(this);
        initFromConfig();
    }

    public void stop() throws Exception {
        if (s3Proxy != null) {
            s3Proxy.stop();
        }
    }

    @VisibleForTesting
    public void useRandomPorts() {
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

    public Clock getClock() {
        return clock;
    }

    @VisibleForTesting
    public void setClock(Clock clock) {
        this.clock = clock;
    }
}
