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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.commons.configuration.event.ConfigurationListener;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BounceService {
    public static final String BOUNCE_POLICY_PREFIX = "bounce.service.bounce-policy";

    private Logger logger = LoggerFactory.getLogger(getClass());
    private Map<String, BounceTaskStatus> bounceStatus = new HashMap<>();
    private ExecutorService executor =
            new ThreadPoolExecutor(1, 1, 0, TimeUnit.DAYS, new LinkedBlockingQueue<>());
    private final BounceApplication app;

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

    @VisibleForTesting
    public synchronized BounceTaskStatus bounce(String container) {
        BounceTaskStatus status = bounceStatus.get(container);
        if (status == null || status.done()) {
            status = new BounceTaskStatus();
            status.future = executor.submit(new BounceTask(container, status));
            bounceStatus.put(container, status);
        }
        return status;
    }

    synchronized BounceTaskStatus fsck(String container) {
        BounceTaskStatus status = bounceStatus.get(container);
        if (status == null || status.done()) {
            status = new BounceTaskStatus();
            status.future = executor.submit(new FsckTask(app.getBlobStore(), container, status));
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

        app.getBlobStore().setPolicy(policy);
    }

    public synchronized void setDefaultPolicy(Optional<BouncePolicy> policy) {
        setDefaultPolicy(policy.orElse(new BounceNothingPolicy()));
    }

    public Clock getClock() {
        return clock;
    }

    @VisibleForTesting
    public void setClock(Clock clock) {
        this.clock = clock;
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
            BounceBlobStore store = app.getBlobStore();
            BouncePolicy policy = store.getPolicy();
            PeekingIterator<StorageMetadata> destinationIterator = Iterators.peekingIterator(
                    Utils.crawlBlobStore(policy.getDestination(), container).iterator());
            PeekingIterator<StorageMetadata> sourceIterator = Iterators.peekingIterator(
                    Utils.crawlBlobStore(store, container).iterator());

            while (destinationIterator.hasNext() && sourceIterator.hasNext()) {
                StorageMetadata destinationObject = destinationIterator.peek();
                BounceStorageMetadata sourceObject = (BounceStorageMetadata) sourceIterator.peek();
                int compare = destinationObject.getName().compareTo(sourceObject.getName());
                if (compare == 0) {
                    // they are the same key
                    reconcileObject(sourceObject, destinationObject);
                    destinationIterator.next();
                    sourceIterator.next();
                } else if (compare < 0) {
                    // near store missing this object
                    reconcileObject(null, destinationObject);
                    destinationIterator.next();
                } else {
                    // destination store missing this object
                    reconcileObject(sourceObject, null);
                    sourceIterator.next();
                }
            }

            sourceIterator.forEachRemaining(meta -> reconcileObject(new BounceStorageMetadata(meta, BounceBlobStore
                    .NEAR_ONLY), null));
            destinationIterator.forEachRemaining(meta -> reconcileObject(null, meta));

            status.endTime = new Date();
        }

        private void reconcileObject(BounceStorageMetadata source, StorageMetadata destination) {
            try {
                adjustCount(app.getBlobStore().getPolicy().reconcileObject(container, source, destination));
            } catch (Throwable e) {
                logger.error("Failed to reconcile object {}, {} in {}: {}", source, destination, container,
                        e.getMessage());
                status.errorObjectCount.getAndIncrement();
            }
            status.totalObjectCount.getAndIncrement();
        }

        private void adjustCount(BounceResult result) {
            switch (result) {
                case MOVE:
                    status.movedObjectCount.getAndIncrement();
                    break;
                case COPY:
                    status.copiedObjectCount.getAndIncrement();
                    break;
                case LINK:
                    status.linkedObjectCount.getAndIncrement();
                    break;
                case REMOVE:
                    status.removedObjectCount.getAndIncrement();
                    break;
                case NO_OP:
                    break;
                default:
                    throw new IllegalStateException("Unknown state: " + result);
            }
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
        final AtomicLong linkedObjectCount = new AtomicLong();
        @JsonProperty
        final Date startTime;
        @JsonProperty
        volatile Date endTime;

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

        public long getLinkedObjectCount() {
            return linkedObjectCount.get();
        }

        public long getRemovedObjectCount() {
            return removedObjectCount.get();
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
    }
}
