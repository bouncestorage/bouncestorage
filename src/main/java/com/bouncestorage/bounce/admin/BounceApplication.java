/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import com.bouncestorage.bounce.BounceBlobStore;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public final class BounceApplication extends Application<BounceConfiguration> {
    private final BounceBlobStore blobStore;

    public BounceApplication(BounceBlobStore blobStore) {
        this.blobStore = blobStore;
    }

    @Override
    public String getName() {
        return "bounce";
    }

    @Override
    public void initialize(Bootstrap<BounceConfiguration> bootstrap) {
        // nothing to do yet
    }

    @Override
    public void run(BounceConfiguration configuration,
            Environment environment) {
        environment.jersey().register(new ServiceResource(blobStore));
        environment.jersey().register(new ContainerResource(blobStore));
        environment.jersey().register(new BounceBlobsResource(blobStore));
    }

}
