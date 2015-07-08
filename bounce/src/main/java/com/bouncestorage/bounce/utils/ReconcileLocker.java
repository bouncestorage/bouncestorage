/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.Pair;

public class ReconcileLocker {
    /**
     * holds the states of operations that are currently happening.
     * the state of the lock is:
     * 0: if it's a reconcile operation
     * 1: if nothing is holding the lock
     * 2+: if one or more threads is doing a non-reconcile operation
     */
    private ConcurrentMap<Pair<String, String>, AtomicLong> operations = new ConcurrentHashMap<>();

    public Object lockObject(String container, String key, boolean reconcile) {
        Pair<String, String> lockKey = Pair.of(container, key);

        do {
            AtomicLong lock = operations.compute(lockKey, (k, v) -> {
                if (v == null) {
                    if (reconcile) {
                        return new AtomicLong(0);
                    } else {
                        return new AtomicLong(2);
                    }
                } else {
                    if (reconcile) {
                        // a non-reconcile operation is happening,
                        // so reconcile is going to skip over this object,
                        // no need to bother with locking
                        return v;
                    } else {
                        if (v.get() != 0) {
                            v.incrementAndGet();
                        }
                        return v;
                    }
                }
            });

            if (reconcile) {
                if (lock.get() != 0) {
                    // something else is going on, that's okay
                    return null;
                } else {
                    return lockKey;
                }
            } else {
                if (lock.get() == 0) {
                    // a reconciling task is working on this key, fail and let the caller
                    // decide what to do
                    return null;
                } else {
                    return lockKey;
                }
            }
        } while (true);
    }

    public void unlockObject(Object lockKey, boolean reconcile) {
        operations.compute((Pair<String, String>) lockKey, (k, v) -> {
            if (reconcile) {
                long state = v.get();
                if (state != 0) {
                    throw new IllegalMonitorStateException("lock state is not 0: " + state);
                }
                return null;
            } else {
                long state = v.decrementAndGet();
                if (state == 1) {
                    // nothing else is going on, we can remove this
                    return null;
                } else if (state == 0) {
                    throw new IllegalMonitorStateException("lock state is 0");
                } else {
                    return v;
                }
            }
        });
    }
}
