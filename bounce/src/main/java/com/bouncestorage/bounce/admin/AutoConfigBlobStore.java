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

        VirtualContainerResource r = new VirtualContainerResource(app);
        VirtualContainer c = new VirtualContainer();
        c.setName(container);
        com.bouncestorage.bounce.admin.Location orig = new com.bouncestorage.bounce.admin.Location();
        orig.setBlobStoreId(0);
        orig.setCopyDelay("-P1D");
        orig.setMoveDelay("-P1D");
        orig.setContainerName(container);
        com.bouncestorage.bounce.admin.Location tier2loc = new com.bouncestorage.bounce.admin.Location();
        tier2loc.setBlobStoreId(1);
        tier2loc.setContainerName(tier2Container);
        c.setOriginLocation(orig);
        c.setArchiveLocation(tier2loc);
        logger.info("auto adding container {}", nContainers);

        boolean res = delegate().createContainerInLocation(location, container, options) |
                tier2.createContainerInLocation(location, tier2Container, options);

        r.createContainer(c);


        return res;
    }
}
