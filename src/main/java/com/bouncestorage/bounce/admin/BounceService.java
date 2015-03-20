/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static java.util.Objects.requireNonNull;

import java.time.Clock;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.StreamSupport;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.BounceStorageMetadata;
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
        this.app = requireNonNull(app);
        if (app.getConfiguration().containsKey(BOUNCE_POLICY_PREFIX)) {
            Optional<BouncePolicy> policy = getBouncePolicyFromName(app.getConfiguration().getString(
                    BOUNCE_POLICY_PREFIX));
            policy.ifPresent(p -> p.init(this, app.getConfiguration().subset(BOUNCE_POLICY_PREFIX)));
            setDefaultPolicy(policy);
        }
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

    synchronized BounceTaskStatus fsck(String container) {
        BounceTaskStatus status = bounceStatus.get(container);
        if (status == null || status.done()) {
            status = new BounceTaskStatus();
            Future<?> future = executor.submit(new FsckTask(app.getBlobStore(), container, status));
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

        bouncePolicy = requireNonNull(policy);
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

    static class AbortedException extends RuntimeException {
    }

    class BounceTask implements Runnable {
        private String container;
        private BounceTaskStatus status;

        BounceTask(String container, BounceTaskStatus status) {
            this.container = requireNonNull(container);
            this.status = requireNonNull(status);
        }

        @Override
        public void run() {
            BounceBlobStore bounceStore = app.getBlobStore();
            try {
                StreamSupport.stream(Utils.crawlBlobStore(bounceStore, container).spliterator(), /*parallel=*/ false)
                        .peek(x -> status.totalObjectCount.getAndIncrement())
                        .map(meta -> (BounceStorageMetadata) meta)
                        .filter(meta -> meta.getRegions() != BounceBlobStore.FAR_ONLY)
                        .filter(bouncePolicy)
                        .forEach(meta -> {
                            if (status.aborted) {
                                throw new AbortedException();
                            }
                            try {
                                BounceResult res = bouncePolicy.bounce(bounceStore, container, meta);
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
            } catch (AbortedException e) {
                status.aborted = true;
            }
            status.endTime = new Date();
        }
    }

    public static final class BounceTaskStatus {
        @JsonProperty
        final AtomicLong totalObjectCount = new AtomicLong();
        @JsonProperty
        final AtomicLong copiedObjectCount = new AtomicLong();
        @JsonProperty
        final AtomicLong movedObjectCount = new AtomicLong();
        @JsonProperty
        final AtomicLong removedObjectCount = new AtomicLong();
        @JsonProperty
        final AtomicLong errorObjectCount = new AtomicLong();
        @JsonProperty
        final Date startTime;
        @JsonProperty
        volatile Date endTime;
        @JsonProperty
        volatile boolean aborted;

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
            if (endTime != null) {
                return (Date) endTime.clone();
            } else {
                return null;
            }
        }

        public void abort() {
            aborted = true;
        }
    }
}
