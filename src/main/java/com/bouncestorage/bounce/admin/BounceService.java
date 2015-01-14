/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.Utils;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.logging.Logger;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.checkNotNull;

public final class BounceService {
    @Resource
    private Logger logger = Logger.NULL;
    private Map<String, BounceTaskStatus> bounceStatus = new HashMap<>();
    private ExecutorService executor =
            new ThreadPoolExecutor(1, 1, 0, TimeUnit.DAYS, new LinkedBlockingQueue<>());
    private BounceBlobStore bounceStore;
    private List<BouncePolicy> bouncePolicies = ImmutableList.of();

    public BounceService(BounceBlobStore bounceStore) {
        this.bounceStore = checkNotNull(bounceStore);
    }

    synchronized BounceTaskStatus bounce(String container) {
        BounceTaskStatus status = bounceStatus.get(container);
        if (status == null || status.done()) {
            status = new BounceTaskStatus();
            Future<?> future = executor.submit(new BounceTask(container, status));
            status.future = future;
            bounceStatus.put(container, status);
        }
        return status;
    }

    synchronized BounceTaskStatus status(String container) {
        return bounceStatus.get(container);
    }

    synchronized Collection<BounceTaskStatus> status() {
        return bounceStatus.values();
    }

    public synchronized void installPolicies(List<BouncePolicy> policies) {
        if (bounceStatus.values().stream().anyMatch(status -> !status.done())) {
            throw new IllegalStateException("Cannot change policies while bouncing objects");
        }

        bouncePolicies = ImmutableList.copyOf(policies);
    }

    class BounceTask implements Runnable {
        private String container;
        private BounceTaskStatus status;

        BounceTask(String container, BounceTaskStatus status) {
            this.container = checkNotNull(container);
            this.status = checkNotNull(status);
        }

        @Override
        public void run() {
            for (StorageMetadata sm : Utils.crawlBlobStore(bounceStore, container)) {
                status.totalObjectCount++;

                if (!bouncePolicies.stream().anyMatch(p -> p.test(sm))) {
                    continue;
                }

                String blobName = sm.getName();
                if (bounceStore.isLink(container, blobName)) {
                    continue;
                }

                try {
                    bounceStore.copyBlobAndCreateBounceLink(container, blobName);
                    status.bouncedObjectCount++;
                } catch (IOException e) {
                    logger.error(e, "could not bounce %s", blobName);
                    status.errorObjectCount++;
                }
            }

            status.endTime = new Date();
        }
    }

    public static final class BounceTaskStatus {
        @JsonProperty
        private volatile long totalObjectCount;
        @JsonProperty
        private volatile long bouncedObjectCount;
        @JsonProperty
        private volatile long errorObjectCount;
        @JsonProperty
        private final Date startTime;
        @JsonProperty
        private volatile Date endTime;

        private Future<?> future;

        public BounceTaskStatus() {
            startTime = new Date();
        }

        public Future<?> future() {
            return future;
        }

        @JsonProperty
        private boolean done() {
            return future.isDone();
        }
    }
}
