/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.ws.rs.ServiceUnavailableException;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReconcileLocker {
    private ConcurrentMap<Object, ReentrantReadWriteLock> operations = new ConcurrentHashMap<>();
    private Logger logger = LoggerFactory.getLogger(getClass());

    public interface LockKey extends AutoCloseable {
        @Override void close();
    }

    public LockKey lockObject(String container, String key, boolean reconcile) {
        Pair<String, String> lockKey = Pair.of(container, key);

        try {
            operations.compute(lockKey, (k, rwLock) -> {
                if (rwLock == null) {
                    rwLock = new ReentrantReadWriteLock();
                    Lock lock = reconcile ? rwLock.writeLock() : rwLock.readLock();
                    lock.lock();
                    return rwLock;
                } else {
                    if (reconcile || rwLock.isWriteLockedByCurrentThread()) {
                        logger.info("concurrent operation {} reconcile: {}", lockKey, reconcile);
                        throw new ServiceUnavailableException("concurrent operation");
                    }
                    Lock lock = rwLock.readLock();
                    if (lock.tryLock()) {
                        return rwLock;
                    } else {
                        logger.info("concurrent operation {} reconcile: {}", lockKey, reconcile);
                        throw new ServiceUnavailableException("concurrent operation");
                    }
                }
            });

            return () -> unlockObject(lockKey, reconcile);
        } catch (RuntimeException e) {
            if (e.getCause() != null && e.getCause() instanceof ServiceUnavailableException) {
                throw (ServiceUnavailableException) e.getCause();
            } else {
                throw e;
            }
        }
    }

    private void unlockObject(Object lockKey, boolean reconcile) {
        operations.compute(lockKey, (k, rwLock) -> {
            if (reconcile) {
                rwLock.writeLock().unlock();
                return null;
            } else {
                rwLock.readLock().unlock();
                if (rwLock.getReadLockCount() == 0) {
                    return null;
                }
                return rwLock;
            }
        });
    }

    @VisibleForTesting
    long size() {
        return operations.size();
    }
}
