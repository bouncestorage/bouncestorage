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

public final class BounceApplication extends Application<BounceConfiguration> {
    private final BounceBlobStore blobStore;
    private final BounceService bounceService;

    public BounceApplication(BounceBlobStore blobStore, BounceService bounceService) {
        this.blobStore = checkNotNull(blobStore);
        this.bounceService = checkNotNull(bounceService);
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
        environment.jersey().register(new ServiceResource(blobStore));
        environment.jersey().register(new ContainerResource(blobStore));
        environment.jersey().register(new BounceBlobsResource(bounceService));
    }

}
