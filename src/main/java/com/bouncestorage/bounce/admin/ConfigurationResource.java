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
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;
import com.google.inject.CreationException;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.logging.Logger;

@Path("/config")
@Produces(MediaType.APPLICATION_JSON)
public final class ConfigurationResource {
    @Resource
    private Logger logger = Logger.NULL;

    private Properties properties;
    private final List<Consumer<BlobStoreContext>> blobStoreListeners = new ArrayList<>();

    public ConfigurationResource(Properties properties) {
        this.properties = checkNotNull(properties);
    }

    private static BlobStoreContext initProperties(Properties props) {
        return ContextBuilder
                .newBuilder("bounce")
                .overrides(props)
                .build(BlobStoreContext.class);
    }

    public void addBlobStoreListener(Consumer<BlobStoreContext> listener) {
        blobStoreListeners.add(listener);
    }

    public void init() {
        useProperties(properties);
    }

    private void useProperties(Properties newProperties) {
        try {
            BlobStoreContext context = initProperties(newProperties);
            blobStoreListeners.forEach(cb -> cb.accept(context));
            properties = newProperties;
        } catch (CreationException e) {
            logger.error("Unable to initialize blob: %s", e.getErrorMessages());
        }
    }

    @POST
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public void updateConfig(Properties newProperties) {
        Properties prop = new Properties();
        prop.putAll(properties);
        prop.putAll(newProperties);
        useProperties(prop);
    }

    @GET
    @Timed
    public Properties getConfig() {
        return properties;
    }
}
