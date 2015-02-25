/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static com.google.common.base.Preconditions.checkNotNull;

import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.StreamSupport;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.admin.BouncePolicy.BounceResult;
import com.bouncestorage.bounce.admin.policy.BounceNothingPolicy;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.configuration.event.ConfigurationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BounceService {
    public static final String BOUNCE_POLICY_PREFIX = "bounce.service.bounce-policy";

    private Logger logger = LoggerFactory.getLogger(getClass());
    private Map<String, BounceTaskStatus> bounceStatus = new HashMap<>();
    private ExecutorService executor =
            new ThreadPoolExecutor(1, 1, 0, TimeUnit.DAYS, new LinkedBlockingQueue<>());
    private final BounceApplication app;
    private BouncePolicy bouncePolicy = new BounceNothingPolicy();

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
                policy.ifPresent(p ->
                        p.init(this, app.getConfiguration().subset(BOUNCE_POLICY_PREFIX)));
                setDefaultPolicy(policy);
            }
        };
    }

    public synchronized void setDefaultPolicy(BouncePolicy policy) {
        if (bounceStatus.values().stream().anyMatch(status -> !status.done())) {
            throw new IllegalStateException("Cannot change policies while bouncing objects");
        }

        bouncePolicy = checkNotNull(policy);
    }

    public synchronized void setDefaultPolicy(Optional<BouncePolicy> policy) {
        setDefaultPolicy(policy.orElse(new BounceNothingPolicy()));
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
                    .map(meta -> bounceStore.blobMetadataNoFollow(container, meta.getName()))
                    .filter(meta -> !BounceLink.isLink(meta))
                    .forEach(meta -> {
                        try {
                            BounceResult res = bouncePolicy.bounce(meta, bounceStore);
                            switch (res) {
                                case MOVE:
                                    status.movedObjectCount.getAndIncrement();
                                    break;
                                case COPY:
                                    status.copiedObjectCount.getAndIncrement();
                                    break;
                                case NO_OP:
                                    break;
                                default:
                                    throw new NullPointerException("res is null");
                            }
                        } catch (Throwable e) {
                            logger.error(String.format("could not bounce %s", meta.getName()), e);
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
        private final AtomicLong copiedObjectCount = new AtomicLong();
        @JsonProperty
        private final AtomicLong movedObjectCount = new AtomicLong();
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

        public long getMovedObjectCount() {
            return movedObjectCount.get();
        }

        public long getCopiedObjectCount() {
            return copiedObjectCount.get();
        }

        public long getErrorObjectCount() {
            return errorObjectCount.get();
        }

        public Date getStartTime() {
            return (Date) startTime.clone();
        }

        public Date getEndTime() {
            return (Date) endTime.clone();
        }
    }
}
