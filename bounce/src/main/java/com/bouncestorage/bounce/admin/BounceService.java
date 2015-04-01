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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.admin.BouncePolicy.BounceResult;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.jclouds.blobstore.BlobStore;
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

    synchronized BounceTaskStatus status(String container) {
        return bounceStatus.get(container);
    }

    synchronized Collection<BounceTaskStatus> status() {
        return bounceStatus.values();
    }

    public Clock getClock() {
        return clock;
    }

    @VisibleForTesting
    public void setClock(Clock clock) {
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
            BlobStore blobStore = app.getBlobStore(container);
            BouncePolicy policy = (BouncePolicy) requireNonNull(blobStore);

            PeekingIterator<StorageMetadata> destinationIterator = Iterators.peekingIterator(
                    Utils.crawlBlobStore(policy.getDestination(), container).iterator());
            PeekingIterator<StorageMetadata> sourceIterator = Iterators.peekingIterator(
                    Utils.crawlBlobStore(policy, container).iterator());

            StorageMetadata destinationObject = Utils.getNextOrNull(destinationIterator);
            BounceStorageMetadata sourceObject = (BounceStorageMetadata) Utils.getNextOrNull(sourceIterator);
            while (destinationObject != null || sourceObject != null) {
                if (destinationObject == null) {
                    reconcileObject(policy, sourceObject, null);
                    sourceObject = (BounceStorageMetadata) Utils.getNextOrNull(sourceIterator);
                    continue;
                } else if (sourceObject == null) {
                    reconcileObject(policy, null, destinationObject);
                    destinationObject = Utils.getNextOrNull(destinationIterator);
                    continue;
                }
                int compare = destinationObject.getName().compareTo(sourceObject.getName());
                if (compare == 0) {
                    // they are the same key
                    reconcileObject(policy, sourceObject, destinationObject);
                    destinationObject = Utils.getNextOrNull(destinationIterator);
                    sourceObject = (BounceStorageMetadata) Utils.getNextOrNull(sourceIterator);
                } else if (compare < 0) {
                    // near store missing this object
                    reconcileObject(policy, null, destinationObject);
                    destinationObject = Utils.getNextOrNull(destinationIterator);
                } else {
                    // destination store missing this object
                    reconcileObject(policy, sourceObject, null);
                    sourceObject = (BounceStorageMetadata) Utils.getNextOrNull(sourceIterator);
                }
            }

            status.endTime = new Date();
        }

        private void reconcileObject(BouncePolicy policy, BounceStorageMetadata source, StorageMetadata destination) {
            try {
                adjustCount(policy.reconcileObject(container, source, destination));
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

        public void abort() {
            aborted = true;
        }
    }
}
