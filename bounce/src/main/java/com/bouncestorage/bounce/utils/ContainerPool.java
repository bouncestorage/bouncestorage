/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.utils;

import static com.google.common.base.Throwables.propagate;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.bouncestorage.bounce.Utils;

import org.jclouds.blobstore.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ContainerPool {
    private static final int CONTAINER_DELAY = 30000;
    private static final int POOL_SIZE = 10;
    private static final int MAX_THREADPOOL_SIZE = 10;
    private static Map<ContainerPoolKey, ContainerPool> poolMap = new HashMap<>();
    private static ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(4, MAX_THREADPOOL_SIZE, 10,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>());
    private static Logger logger = LoggerFactory.getLogger(ContainerPool.class);

    private BlobStore blobStore;
    private BlockingQueue<String> containerPool;
    private boolean eventualConsistency = true;

    private ContainerPool(BlobStore blobStore) {
        this.blobStore = blobStore;
        // this is probably a local swift which is always consistent
        if ("test:tester".equals(blobStore.getContext().unwrap().getIdentity())) {
            eventualConsistency = false;
        } else {
            blobStore.list().stream().parallel()
                    .map(m -> m.getName())
                    .filter(n -> n.matches(Utils.RANDOM_CONTAINER_REGEX))
                    .forEach(n -> {
                        try {
                            blobStore.deleteContainer(n);
                        } catch (RuntimeException e) {
                            logger.error("unable to remove container {}", n);
                        }
                    });
        }
        this.containerPool = new LinkedBlockingQueue<>();
        for (int i = 0; i < POOL_SIZE; i++) {
            createContainer(false);
        }
        awaitConsistency();
    }

    private void awaitConsistency() {
        if (eventualConsistency) {
            try {
                Thread.sleep(CONTAINER_DELAY);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    public String getContainer() {
        String container;
        try {
            container = containerPool.take();
        } catch (InterruptedException e) {
            throw propagate(e);
        }
        if (containerPool.size() < POOL_SIZE) {
            threadPoolExecutor.execute(() -> createContainer(true));
        }

        return container;
    }

    private void createContainer(boolean shouldSleep) {
        String container = Utils.createRandomContainerName();
        blobStore.createContainerInLocation(null, container);
        if (shouldSleep) {
            awaitConsistency();
        }
        containerPool.add(container);
    }

    public static ContainerPool getContainerPool(BlobStore blobStore) {
        ContainerPoolKey key = ContainerPoolKey.create(blobStore);
        ContainerPool entry;
        synchronized (poolMap) {
            if (!poolMap.containsKey(key)) {
                poolMap.put(key, new ContainerPool(blobStore));
            }
            entry = poolMap.get(key);
        }
        return entry;
    }

    public static void destroyAllContainers() {
        threadPoolExecutor.shutdown();
        try {
            threadPoolExecutor.awaitTermination(CONTAINER_DELAY, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("Interrupted in container pool shutdown: " + e, e);
        }
        poolMap.forEach((blobStoreId, containerPool) -> {
            containerPool.containerPool.forEach(container -> {
                try {
                    containerPool.blobStore.deleteContainer(container);
                    logger.debug("deleted: " + container);
                } catch (Exception e) {
                    logger.error("Failed to delete: " + container, e);
                }
            });
        });
    }
}
