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
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import com.bouncestorage.bounce.BlobStoreTarget;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
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


public final class BounceApplication extends Application<BounceDropWizardConfiguration> {
    private static final ImmutableSet<String> REQUIRED_PROPERTIES =
            ImmutableSet.of(S3ProxyConstants.PROPERTY_ENDPOINT);

    private Logger logger = LoggerFactory.getLogger(getClass());

    private final BounceConfiguration config;
    private final Properties configView;

    private BounceService bounceService;
    private int port = -1;
    private boolean useRandomPorts;
    private S3Proxy s3Proxy;
    private Map<Integer, BlobStore> providers = new HashMap<>();
    private Map<String, BouncePolicy> virtualContainers = new HashMap<>();
    private Map<String, Map<String, String>> vContainerCredentials = new HashMap<>();
    private final Pattern providerConfigPattern = Pattern.compile("(bounce.backend.\\d+).jclouds.provider");
    private final Pattern containerConfigPattern = Pattern.compile("(bounce.container.\\d+).name");
    private Clock clock = Clock.systemUTC();
    private ScheduledExecutorService backgroundTasks = Executors.newScheduledThreadPool(4);

    public BounceApplication() {
        this.config = new BounceConfiguration();
        this.configView = new ConfigurationPropertiesView(config);
    }

    private void startS3Proxy() {
        try {
            if (s3Proxy != null) {
                stop();
            }
            URI endpoint = new URI(config.getString(S3ProxyConstants.PROPERTY_ENDPOINT));
            S3Proxy.Builder builder = S3Proxy.builder()
                    .endpoint(endpoint);

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
            logger.info("Starting S3Proxy on {}", endpoint);
            s3Proxy.start();
        } catch (Exception e) {
            throw propagate(e);
        }
    }

    private void addProviderFromConfig(String prefix, String provider) {
        String id = prefix.substring(prefix.lastIndexOf('.')).substring(1);
        logger.info("adding provider from {} id: {}", prefix, id);
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

        providers.put(Integer.valueOf(id), context.getBlobStore());
    }

    static Optional<BouncePolicy> getBouncePolicyFromName(String name) {
        ServiceLoader<BouncePolicy> loader = ServiceLoader.load(BouncePolicy.class);
        return StreamSupport.stream(loader.spliterator(), false)
                .filter(p -> p.getClass().getSimpleName().equals(name))
                .findAny();
    }

    private void addContainerFromConfig(String prefix, String containerName) {
        logger.debug("adding container {} from {}", containerName, prefix);
        Configuration c = config.subset(prefix);
        int maxTierID = 3;
        BlobStore lastBlobStore = null;
        BouncePolicy lastPolicy = null;
        for (int i = maxTierID; i >= 0; i--) {
            String tierPrefix = "tier." + i;
            if (!c.containsKey(tierPrefix + "." + Location.BLOB_STORE_ID_FIELD)) {
                continue;
            }
            int id = c.getInt(tierPrefix + "." + Location.BLOB_STORE_ID_FIELD);
            BlobStore store = providers.get(id);
            String targetContainerName = c.getString(tierPrefix + "." + Location.CONTAINER_NAME_FIELD, containerName);
            if (lastBlobStore == null) {
                lastBlobStore = new BlobStoreTarget(store, targetContainerName);
            } else {
                String policyName = c.getString(tierPrefix + ".policy");
                BouncePolicy policy = getBouncePolicyFromName(policyName)
                        .orElseThrow(() -> propagate(new ClassNotFoundException(policyName)));

                policy.init(this, c.subset(tierPrefix));
                policy.setBlobStores(new BlobStoreTarget(store, targetContainerName),
                        lastBlobStore);
                lastBlobStore = policy;
                lastPolicy = policy;
            }
        }
        if (lastPolicy == null) {
            throw new NoSuchElementException("not enough configured tiers");
        }
        virtualContainers.put(containerName, lastPolicy);
        if (c.containsKey("identity")) {
            vContainerCredentials.put(containerName,
                    ImmutableMap.of(c.getString("identity"), c.getString("credential")));
        }
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

    public BlobStore getBlobStore(int providerId) {
        if (providers == null) {
            return null;
        }
        return providers.get(providerId);
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

    @VisibleForTesting
    public Map.Entry<String, BlobStore> locateBlobStore(String identity,
                                                        String container, String blob) {
        BlobStore blobStore = null;
        Optional<String> credential = Optional.empty();

        if (container != null) {
            blobStore = virtualContainers.get(container);

            if (blobStore != null) {
                Map<String, String> creds = vContainerCredentials.get(container);
                if (creds != null) {
                    credential = Optional.ofNullable(creds.get(identity));
                }
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

    public BounceConfiguration getConfiguration() {
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
    public void initialize(Bootstrap<BounceDropWizardConfiguration> bootstrap) {
        bootstrap.addBundle(new AssetsBundle("/assets", "/", "views/index.html"));
        bootstrap.setConfigurationSourceProvider(new UrlConfigurationSourceProvider());
    }

    @Override
    public void run(BounceDropWizardConfiguration configuration, Environment environment) {
        environment.jersey().setUrlPattern("/api/*");
        environment.jersey().register(new ServiceResource(this));
        environment.jersey().register(new ContainerResource(this));
        environment.jersey().register(new BounceBlobsResource(this));
        environment.jersey().register(new ConfigurationResource(this));
        environment.jersey().register(new ObjectStoreResource(this));
        environment.jersey().register(new VirtualContainerResource(this));
        environment.jersey().register(new SettingsResource(this));
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
        registerConfigurationListener();
    }

    public void stop() throws Exception {
        if (s3Proxy != null) {
            logger.info("Stopping S3Proxy");
            s3Proxy.stop();
        }
        if (!backgroundTasks.isShutdown()) {
            backgroundTasks.shutdown();
        }
    }

    @VisibleForTesting
    public void registerConfigurationListener() {
        config.addConfigurationListener(evt -> {
            String name = evt.getPropertyName();
            Matcher m;

            if (!evt.isBeforeUpdate()) {
                if ((m = providerConfigPattern.matcher(name)).matches()) {
                    addProviderFromConfig(m.group(1), (String) evt.getPropertyValue());
                } else if ((m = containerConfigPattern.matcher(name)).matches()) {
                    addContainerFromConfig(m.group(1), (String) evt.getPropertyValue());
                } else if (S3ProxyConstants.PROPERTY_ENDPOINT.equals(name)) {
                    startS3Proxy();
                }
            }
        });
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

    public <T> Future<T> executeBackgroundTask(Callable<T> task) {
        return backgroundTasks.submit(task);
    }

    public <T> ScheduledFuture<T> executeBackgroundTask(Callable<T> task, long delay, TimeUnit unit) {
        return backgroundTasks.schedule(task, delay, unit);
    }

    @VisibleForTesting
    public void drainBackgroundTasks() throws InterruptedException {
        backgroundTasks.shutdown();
        backgroundTasks.awaitTermination(60, TimeUnit.SECONDS);
    }
}
