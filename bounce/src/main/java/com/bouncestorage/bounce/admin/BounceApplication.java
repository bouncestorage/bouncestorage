/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Throwables.propagate;

import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;
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
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import com.bouncestorage.bounce.BlobStoreTarget;
import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.PausableThreadPoolExecutor;
import com.bouncestorage.bounce.utils.KeyStoreUtils;
import com.bouncestorage.swiftproxy.SwiftProxy;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.inject.CreationException;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.configuration.UrlConfigurationSourceProvider;
import io.dropwizard.logging.LoggingFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.bouncycastle.operator.OperatorCreationException;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.gaul.s3proxy.S3Proxy;
import org.gaul.s3proxy.S3ProxyConstants;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;


public final class BounceApplication extends Application<BounceDropWizardConfiguration> {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private final BounceConfiguration config;
    private final Properties configView;

    private BounceService bounceService;
    private int port = -1;
    private boolean useRandomPorts;
    private S3Proxy s3Proxy;
    private SwiftProxy swiftProxy;
    private Map<Integer, BlobStore> providers = new HashMap<>();
    private Map<String, BouncePolicy> virtualContainers = new HashMap<>();
    private Map<String, VirtualContainer> vContainerConfig = new HashMap<>();
    private final Pattern providerConfigPattern = Pattern.compile("(bounce.backend.\\d+).jclouds.provider");
    private final Pattern containerConfigPattern = Pattern.compile("(bounce.container.\\d+).name");
    private Clock clock = Clock.systemUTC();
    private PausableThreadPoolExecutor backgroundReconcileTasks = new PausableThreadPoolExecutor(4);
    private PausableThreadPoolExecutor backgroundTasks = new PausableThreadPoolExecutor(4);
    private BounceStats bounceStats;
    private KeyStoreUtils keyStoreUtils;

    public BounceApplication() {
        this.config = new BounceConfiguration();
        this.configView = new ConfigurationPropertiesView(config);
        bounceStats = new BounceStats();
    }

    public BounceApplication(String configurationFile) {
        try {
            this.config = new BounceConfiguration(configurationFile);
        } catch (ConfigurationException e) {
            throw propagate(e);
        }
        this.configView = new ConfigurationPropertiesView(config);
        bounceStats = new BounceStats();
    }

    private void startSwiftProxy() {
        try {
            if (swiftProxy != null) {
                swiftProxy.stop();
            }
            String endpointString = config.getString(SwiftProxy.PROPERTY_ENDPOINT);
            if (endpointString == null || endpointString.equals("")) {
                logger.warn("Swift endpoint not set");
                return;
            }
            URI endpoint = new URI(endpointString);
            SwiftProxy.Builder builder = SwiftProxy.Builder.builder().endpoint(endpoint).locator(
                    (identity, container, object) -> locateBlobStore(identity, container, object));
            swiftProxy = builder.build();
            logger.info("Starting Swift proxy");
            swiftProxy.start();
        } catch (Exception e) {
            throw propagate(e);
        }
    }

    private void startS3Proxy() {
        try {
            if (s3Proxy != null) {
                logger.info("Stopping S3Proxy");
                s3Proxy.stop();
            }

            String endpoint = config.getString(S3ProxyConstants.PROPERTY_ENDPOINT);
            String secureEndpoint = config.getString(S3ProxyConstants.PROPERTY_SECURE_ENDPOINT);
            String authorization = config.getString(S3ProxyConstants.PROPERTY_AUTHORIZATION);

            if ((Strings.isNullOrEmpty(endpoint) && Strings.isNullOrEmpty(secureEndpoint)) ||
                    Strings.isNullOrEmpty(authorization)) {
                logger.warn("S3 endpoint and authorization must be set");
                return;
            }

            if (!authorization.equalsIgnoreCase("aws-v2") && !authorization.equalsIgnoreCase("none")) {
                logger.warn("S3 authorization must be 'none' or 'aws-v2'");
                return;
            }

            S3Proxy.Builder builder = S3Proxy.builder();
            if (!Strings.isNullOrEmpty(endpoint)) {
                builder.endpoint(new URI(endpoint));
            }
            if (!Strings.isNullOrEmpty(secureEndpoint)) {
                builder.secureEndpoint(new URI(secureEndpoint));
            }
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
                initKeyStore();
                generateSelfSignedCert();
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
            e.printStackTrace();
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
                    .modules(ImmutableList.of(new SLF4JLoggingModule()))
                    .build(BlobStoreContext.class);
            logger.info("added provider {} id {}", context.unwrap().getId(), id);
        } catch (CreationException e) {
            e.printStackTrace();
            throw propagate(e);
        }

        providers.put(Integer.valueOf(id), new LoggingBlobStore(context.getBlobStore(), this));
    }

    static Optional<BouncePolicy> getBouncePolicyFromName(String name) {
        ServiceLoader<BouncePolicy> loader = ServiceLoader.load(BouncePolicy.class);
        return StreamSupport.stream(loader.spliterator(), false)
                .filter(p -> p.getClass().getSimpleName().equals(name))
                .findAny();
    }

    private static int prefixToId(String prefix) {
        int dot = prefix.lastIndexOf('.', prefix.length() - 2);
        if (dot == -1) {
            throw new IllegalArgumentException(prefix);
        }

        return Integer.valueOf(prefix.substring(dot + 1));
    }

    private void addContainerFromConfig(String prefix, String containerName) {
        logger.debug("adding container {} from {}", containerName, prefix);
        Configuration c = config.subset(prefix);
        int containerId = prefixToId(prefix);
        int maxTierID = 3;
        BlobStore lastBlobStore = null;
        BouncePolicy lastPolicy = null;
        VirtualContainer virtualContainer = vContainerConfig.get(containerName);
        if (virtualContainer == null) {
            virtualContainer = new VirtualContainer();
            virtualContainer.setName(containerName);
            vContainerConfig.put(containerName, virtualContainer);
            virtualContainer.setId(containerId);
        } else {
            if (virtualContainer.getId() != containerId) {
                throw new IllegalArgumentException(
                        String.format("Cannot update container id from %d to %d",
                                virtualContainer.getId(), containerId));
            }
        }

        for (int i = maxTierID; i >= 0; i--) {
            String tierPrefix = "tier." + i;
            int id = c.getInt(tierPrefix + "." + Location.BLOB_STORE_ID_FIELD, -1);
            if (id == -1) {
                continue;
            }
            BlobStore store = providers.get(id);
            if (store == null) {
                throw new IllegalArgumentException(String.format("Blobstore %d not found", id));
            }
            virtualContainer.getLocation(i).setBlobStoreId(id);
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
            virtualContainer.identity = c.getString("identity");
            virtualContainer.credential = c.getString("credential");
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
        return virtualContainers.get(containerName);
    }

    public BounceStats getBounceStats() {
        return bounceStats;
    }

    @VisibleForTesting
    public Map.Entry<String, BlobStore> locateBlobStore(String identity,
                                                        String container, String blob) {
        BlobStore blobStore = null;
        String credential = null;

        if (container != null) {
            blobStore = virtualContainers.get(container);

            if (blobStore != null) {
                VirtualContainer virtualContainer = vContainerConfig.get(container);
                if (identity.equals(virtualContainer.identity)) {
                    credential = virtualContainer.credential;
                } else {
                    for (int i = 0; i < 3; i++) {
                        Location loc = virtualContainer.getLocation(i);
                        int blobstoreId = loc.getBlobStoreId();
                        Configuration c = config.subset(BounceBlobStore.STORE_PROPERTY + "." + blobstoreId);
                        if (identity.equals(c.getString(Constants.PROPERTY_IDENTITY))) {
                            credential = c.getString(Constants.PROPERTY_CREDENTIAL);
                        }
                    }
                }
            }
        }

        if (blobStore == null) {
            List<Object> backendIDs = config.getList(BounceBlobStore.STORES_LIST);
            logger.info("keys: {}", backendIDs);
            for (Object id : backendIDs) {
                Configuration c = config.subset(BounceBlobStore.STORE_PROPERTY + "." + id);
                if (identity.equals(c.getString(Constants.PROPERTY_IDENTITY))) {
                    credential = c.getString(Constants.PROPERTY_CREDENTIAL);
                    blobStore = providers.get(Integer.valueOf(id.toString()));
                }
            }
        }

        if (blobStore != null && credential != null) {
            logger.info("identity: {} credential: {}", identity, credential);
            return Maps.immutableEntry(credential, blobStore);
        }

        return null;
    }

    private void initFromConfig() {
        try {
            startS3Proxy();
        } catch (Throwable t) {
            t.printStackTrace();
        }
        try {
            startSwiftProxy();
        } catch (Throwable t) {
            t.printStackTrace();
        }
        config.getList(BounceBlobStore.STORES_LIST).forEach(id -> {
            try {
                addProviderFromConfig("bounce.backend." + id,
                        config.getString("bounce.backend." + id + ".jclouds.provider"));
            } catch (Throwable e) {
                logger.error("Failed to initialize provider " + id + ": " + e.getMessage());
                e.printStackTrace();
            }
        });
        config.getList(VirtualContainerResource.CONTAINERS_PREFIX).forEach(id -> {
            try {
                addContainerFromConfig("bounce.container." + id,
                        config.getString("bounce.container." + id + ".name"));
            } catch (Throwable e) {
                logger.error("Failed to initialize container " + id + ": " + e.getMessage());
                e.printStackTrace();
            }
        });
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
        if (s3Proxy == null) {
            return AbstractLifeCycle.STOPPED;
        }
        return s3Proxy.getState();
    }

    public boolean isSwiftProxyStarted() {
        if (swiftProxy == null) {
            return false;
        }
        return swiftProxy.isStarted();
    }

    public int getS3ProxyPort() {
        return s3Proxy.getPort();
    }

    public int getSwiftPort() {
        return requireNonNull(swiftProxy).getPort();
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
        registerConfigurationListener();
        bounceService = new BounceService(this);
        initFromConfig();
        bounceStats.start();
    }

    public void stop() throws Exception {
        if (s3Proxy != null) {
            logger.info("Stopping S3Proxy");
            s3Proxy.stop();
        }
        if (!backgroundReconcileTasks.isShutdown()) {
            backgroundReconcileTasks.shutdown();
        }
        bounceStats.shutdown();
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
                } else if (S3ProxyConstants.PROPERTY_SECURE_ENDPOINT.equals(name)) {
                    startS3Proxy();
                } else if (SwiftProxy.PROPERTY_ENDPOINT.equals(name)) {
                    startSwiftProxy();
                }
            }
        });
    }

    public KeyStoreUtils getKeyStoreUtils() {
        return keyStoreUtils;
    }

    private void generateSelfSignedCert() {
        String virtualHost = config.getString(S3ProxyConstants.PROPERTY_VIRTUAL_HOST);
        if (!Strings.isNullOrEmpty(virtualHost)) {
            try {
                keyStoreUtils.ensureCertificate("*." + virtualHost);
            } catch (GeneralSecurityException | IOException | OperatorCreationException e) {
                throw propagate(e);
            }
        }
    }

    void initTestingKeyStore() {
        try {
            keyStoreUtils = KeyStoreUtils.getTestingKeyStore();
        } catch (GeneralSecurityException | IOException e) {
            throw propagate(e);
        }
    }

    private void initKeyStore() {
        try {
            keyStoreUtils = KeyStoreUtils.getKeyStore(
                    config.getString(S3ProxyConstants.PROPERTY_KEYSTORE_PATH),
                    config.getString(S3ProxyConstants.PROPERTY_KEYSTORE_PASSWORD));
        } catch (GeneralSecurityException | IOException e) {
            throw propagate(e);
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

    public <T> Future<T> executeBackgroundTask(Callable<T> task) {
        return backgroundTasks.submit(task);
    }

    public <T> ScheduledFuture<T> executeBackgroundReconcileTask(Callable<T> task, long delay, TimeUnit unit) {
        return backgroundReconcileTasks.schedule(task, delay, unit);
    }

    @VisibleForTesting
    public void drainBackgroundTasks() throws InterruptedException {
        backgroundReconcileTasks.shutdown();
        backgroundReconcileTasks.awaitTermination(60, TimeUnit.SECONDS);
    }

    @VisibleForTesting
    public void pauseBackgroundTasks() {
        backgroundReconcileTasks.pause();
    }

    @VisibleForTesting
    public void resumeBackgroundTasks() {
        backgroundReconcileTasks.resume();
    }
}
