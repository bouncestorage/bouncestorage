/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.Utils;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.configuration.event.ConfigurationListener;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.logging.Logger;

import javax.annotation.Resource;
import java.io.IOException;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;

public final class BounceService {
    public static final String BOUNCE_POLICY_PREFIX = "bounce.service.bounce-policy";
    @Resource
    private Logger logger = Logger.NULL;
    private Map<String, BounceTaskStatus> bounceStatus = new HashMap<>();
    private ExecutorService executor =
            new ThreadPoolExecutor(1, 1, 0, TimeUnit.DAYS, new LinkedBlockingQueue<>());
    private final BounceApplication app;
    private Predicate<StorageMetadata> bouncePolicy = x -> false;

    private Clock clock = Clock.systemUTC();

    public BounceService(BounceApplication app) {
        this.app = checkNotNull(app);
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

    static Optional<BouncePolicy> getBouncePolicyFromName(String name) {
        ServiceLoader<BouncePolicy> loader = ServiceLoader.load(BouncePolicy.class);
        return StreamSupport.stream(loader.spliterator(), false)
                .filter(p -> p.getClass().getSimpleName().equals(name))
                .findAny();
    }

    ConfigurationListener getConfigurationListener() {
        return event -> {
            if (event.getPropertyName().equals(BOUNCE_POLICY_PREFIX)) {
                Optional<BouncePolicy> policy =
                        getBouncePolicyFromName((String) event.getPropertyValue());
                if (policy.isPresent()) {
                    policy.get().init(this, app.getConfiguration().subset(BOUNCE_POLICY_PREFIX));
                    installPolicies(ImmutableSet.of(policy.get()));
                } else {
                    installPolicies(ImmutableSet.of());
                }
            }
        };
    }

    public synchronized void installPolicies(Collection<Predicate<StorageMetadata>> policies) {
        if (bounceStatus.values().stream().anyMatch(status -> !status.done())) {
            throw new IllegalStateException("Cannot change policies while bouncing objects");
        }

        bouncePolicy = policies.stream().reduce(Predicate::or).orElse(x -> false);
    }

    public Clock getClock() {
        return clock;
    }

    void setClock(Clock clock) {
        this.clock = clock;
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
            BounceBlobStore bounceStore = app.getBlobStore();
            StreamSupport.stream(Utils.crawlBlobStore(bounceStore, container).spliterator(), /*parallel=*/ false)
                    .peek(x -> status.totalObjectCount.getAndIncrement())
                    .filter(bouncePolicy)
                    .map(StorageMetadata::getName)
                    .filter(blobName -> !bounceStore.isLink(container, blobName))
                    .forEach(blobName -> {
                        try {
                            bounceStore.copyBlobAndCreateBounceLink(container, blobName);
                            status.bouncedObjectCount.getAndIncrement();
                        } catch (IOException e) {
                            logger.error(e, "could not bounce %s", blobName);
                            status.errorObjectCount.getAndIncrement();
                        }
                    });

            status.endTime = new Date();
        }
    }

    public static final class BounceTaskStatus {
        @JsonProperty
        private final AtomicLong totalObjectCount = new AtomicLong();
        @JsonProperty
        private final AtomicLong bouncedObjectCount = new AtomicLong();
        @JsonProperty
        private final AtomicLong errorObjectCount = new AtomicLong();
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

        public long getTotalObjectCount() {
            return totalObjectCount.get();
        }

        public long getBouncedObjectCount() {
            return bouncedObjectCount.get();
        }

        public long getErrorObjectCount() {
            return errorObjectCount.get();
        }

        public Date getStartTime() {
            return startTime;
        }

        public Date getEndTime() {
            return endTime;
        }
    }
}
