/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Properties;

import com.bouncestorage.bounce.IForwardingBlobStore;
import com.bouncestorage.bounce.utils.ContainerPool;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.domain.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutoConfigBlobStore implements IForwardingBlobStore {
    private final BlobStore delegate;
    private final BounceApplication app;
    private Logger logger = LoggerFactory.getLogger(getClass());

    AutoConfigBlobStore(BlobStore delegate, BounceApplication app) {
        this.delegate = requireNonNull(delegate);
        this.app = requireNonNull(app);
    }

    @Override
    public BlobStore delegate() {
        return delegate;
    }

    @Override
    public boolean createContainerInLocation(Location location, String container, CreateContainerOptions options) {
        BounceConfiguration config = app.getConfiguration();
        List<Object> containers = config.getList(VirtualContainerResource.CONTAINERS_PREFIX);
        logger.debug("existing containers: {}", config.getString("bounce.containers"));
        Properties p = new Properties();
        int nContainers;
        if (containers == null || containers.isEmpty()) {
            nContainers = 1;
            p.setProperty("bounce.containers", "1");
        } else {
            nContainers = containers.stream()
                    .mapToInt(o -> Integer.parseInt(o.toString())).max().orElse(0) + 1;
            p.setProperty("bounce.containers", config.getString("bounce.containers") + "," + nContainers);
        }

        BlobStore tier2 = ((IForwardingBlobStore) app.getBlobStore(1)).delegate();
        String tier2Container = ContainerPool.getContainerPool(tier2).getContainer();
        String prefix = "bounce.container." + nContainers;
        p.setProperty(prefix + ".name", container);
        p.setProperty(prefix + ".tier.1.evictDelay", "-P1D");
        p.setProperty(prefix + ".tier.1.copyDelay", "-P1D");
        p.setProperty(prefix + ".tier.1.container", container);
        p.setProperty(prefix + ".tier.1.backend", "0");
        p.setProperty(prefix + ".tier.1.policy", "WriteBackPolicy");
        p.setProperty(prefix + ".tier.2.container", tier2Container);
        p.setProperty(prefix + ".tier.2.backend", "1");
        logger.info("auto adding container {}", nContainers);

        boolean res = delegate().createContainerInLocation(location, container, options) |
                tier2.createContainerInLocation(location, tier2Container, options);

        config.setAll(p);

        return res;
    }
}
