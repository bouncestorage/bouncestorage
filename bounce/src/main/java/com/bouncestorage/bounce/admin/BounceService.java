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
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.StreamSupport;

import com.bouncestorage.bounce.BlobStoreTarget;
import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.admin.BouncePolicy.BounceResult;
import com.bouncestorage.bounce.admin.policy.WriteBackPolicy;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.commons.lang3.tuple.Pair;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BounceService {
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
        return bounce(container, executor);
    }

    @VisibleForTesting
    public synchronized BounceTaskStatus bounce(String container, ExecutorService exe) {
        BounceTaskStatus status = bounceStatus.get(container);
        if (status == null || status.done()) {
            status = new BounceTaskStatus();
            status.container = container;
            status.future = exe.submit(new BounceTask(container, status));
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

    class BounceTask implements Runnable {
        private String container;
        private BounceTaskStatus status;
        private ContainerStats sourceStats;
        private ContainerStats destinationStats;
        private Throwable initBackTrace;

        BounceTask(String container, BounceTaskStatus status) {
            this.container = requireNonNull(container);
            this.status = requireNonNull(status);
            sourceStats = new ContainerStats();
            destinationStats = new ContainerStats();
            initBackTrace = new RuntimeException();
        }

        @Override
        public void run() {
            try {
                BlobStore blobStore = app.getBlobStore(container);
                BouncePolicy policy = (BouncePolicy) requireNonNull(blobStore);
                processPolicy(policy);
                logStats(policy.getSource(), sourceStats);
                if (policy.getDestination() instanceof BouncePolicy) {
                    processPolicy((BouncePolicy) policy.getDestination());
                } else if (policy.getDestination() != null) {
                    logStats(policy.getDestination(), destinationStats);
                }
            } catch (Throwable e) {
                e.initCause(initBackTrace);
                logger.error("bounce error", e);
                status.aborted = true;
                status.errorObjectCount.incrementAndGet();
            } finally {
                status.endTime = new Date();
            }
        }

        private void logStats(BlobStore blobStore, ContainerStats stats) {
            String targetContainer = container;
            if (blobStore instanceof BlobStoreTarget) {
                targetContainer = ((BlobStoreTarget) blobStore).mapContainer(null);
            }
            app.getBounceStats().logObjectStoreStats(app.getBlobStoreId(blobStore), targetContainer, stats.totalSize,
                    stats.objectCount);
        }

        class ReconcileIterator implements Iterator<Pair<BounceStorageMetadata, StorageMetadata>> {
            PeekingIterator<StorageMetadata> srcIter;
            PeekingIterator<StorageMetadata> destIter;

            ReconcileIterator(PeekingIterator<StorageMetadata> srcIter, PeekingIterator<StorageMetadata> destIter) {
                this.srcIter = requireNonNull(srcIter);
                this.destIter = requireNonNull(destIter);
            }

            @Override
            public boolean hasNext() {
                return !status.aborted && (srcIter.hasNext() || destIter.hasNext());
            }

            private StorageMetadata peekOrNull(PeekingIterator<StorageMetadata> iter) {
                return iter.hasNext() ? iter.peek() : null;
            }

            @Override
            public Pair<BounceStorageMetadata, StorageMetadata> next() {
                BounceStorageMetadata src = (BounceStorageMetadata) peekOrNull(srcIter);
                StorageMetadata dest = peekOrNull(destIter);
                if (dest == null) {
                    srcIter.next();
                } else if (src == null) {
                    destIter.next();
                } else {
                    int compare = dest.getName().compareTo(src.getName());
                    if (compare == 0) {
                        srcIter.next();
                        destIter.next();
                    } else if (compare < 0) {
                        src = null;
                        destIter.next();
                    } else {
                        dest = null;
                        srcIter.next();
                    }
                }
                return Pair.of(src, dest);
            }
        }

        private void processPolicy(BouncePolicy policy) {
            logger.info("processing policy {} {}", policy.getClass(), status.container);

            ListContainerOptions options = new ListContainerOptions().recursive();

            PeekingIterator<StorageMetadata> destinationIterator = Iterators.peekingIterator(
                    Utils.crawlBlobStore(policy.getDestination(), container, options).iterator());
            PeekingIterator<StorageMetadata> sourceIterator = Iterators.peekingIterator(
                    Utils.crawlBlobStore(policy, container, options).iterator());

            policy.prepareBounce(container);
            StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                    new ReconcileIterator(sourceIterator, destinationIterator), Spliterator.CONCURRENT), true)
                    .filter(p -> p.getRight() == null || !WriteBackPolicy.isSwiftSegmentBlob(p.getRight().getName()))
                    .forEach((p) -> {
                        BounceStorageMetadata sourceObject = p.getLeft();
                        StorageMetadata destinationObject = p.getRight();

                        reconcileObject(policy, sourceObject, destinationObject);
                    });
        }

        private void reconcileObject(BouncePolicy policy, BounceStorageMetadata source,
                                             StorageMetadata destination) {
            try {
                BounceResult result = policy.reconcileObject(container, source, destination);
                adjustContainerStats(result, source, destination);
                adjustCount(result);
            } catch (Throwable e) {
                e.printStackTrace();
                logger.error("Failed to reconcile object {}, {} in {}: {}", source, destination, container,
                        e.getMessage());
                status.errorObjectCount.getAndIncrement();
            } finally {
                status.totalObjectCount.getAndIncrement();
            }
        }

        private void adjustContainerStats(BounceResult result, StorageMetadata source, StorageMetadata destination) {
            if (source != null && result != BounceResult.REMOVE) {
                // We may remove the object from the source during a migration operation
                sourceStats.objectCount += 1;
                switch (result) {
                    case COPY:
                        destinationStats.objectCount += 1;
                        destinationStats.totalSize += source.getSize();
                        sourceStats.totalSize += source.getSize();
                        break;
                    case NO_OP:
                        sourceStats.totalSize += source.getSize();
                        break;
                    case MOVE:
                    case LINK:
                        // TODO: we should look up the link size
                        sourceStats.totalSize += 0;
                        destinationStats.objectCount += 1;
                        destinationStats.totalSize += source.getSize();
                        break;
                    default:
                        break;
                }
            }
            if (destination != null && source == null && result != BounceResult.REMOVE) {
                // This can only be a NO_OP at this point (COPY, LINK, or MOVE would be taken care of above.
                destinationStats.objectCount += 1;
                destinationStats.totalSize += destination.getSize();
            }
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
        String container;
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

        public String getContainer() {
            return container;
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

    public static final class ContainerStats {
        // Can represent up to ~9 exabytes as bytes
        long totalSize;
        long objectCount;
    }
}
