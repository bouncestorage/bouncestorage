/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;

import javax.ws.rs.ServiceUnavailableException;

import org.junit.Test;

public class ReconcileLockerTest {
    ReconcileLocker reconcileLocker = new ReconcileLocker();

    @Test
    public void testReadRead() {
        ReconcileLocker.LockKey lock1 = reconcileLocker.lockObject("container", "key", false);
        ReconcileLocker.LockKey lock2 = reconcileLocker.lockObject("container", "key", false);
        lock1.close();
        lock2.close();
        assertThat(reconcileLocker.size()).isEqualTo(0);
    }

    @Test
    public void testReadWrite() {
        ReconcileLocker.LockKey lock1 = reconcileLocker.lockObject("container", "key", false);
        assertThatThrownBy(() -> reconcileLocker.lockObject("container", "key", true))
                .isInstanceOf(ServiceUnavailableException.class);
        lock1.close();
        assertThat(reconcileLocker.size()).isEqualTo(0);
    }

    @Test
    public void testWriteRead() {
        ReconcileLocker.LockKey lock1 = reconcileLocker.lockObject("container", "key", true);
        assertThatThrownBy(() -> reconcileLocker.lockObject("container", "key", false))
                .isInstanceOf(ServiceUnavailableException.class);
        lock1.close();
        assertThat(reconcileLocker.size()).isEqualTo(0);
    }

    @Test
    public void testWriteWrite() {
        ReconcileLocker.LockKey lock1 = reconcileLocker.lockObject("container", "key", true);
        assertThatThrownBy(() -> reconcileLocker.lockObject("container", "key", true))
                .isInstanceOf(ServiceUnavailableException.class);
        lock1.close();
        assertThat(reconcileLocker.size()).isEqualTo(0);
    }

    @Test
    public void testReadBench() {
        int num = 65535;
        ArrayList<ReconcileLocker.LockKey> locks = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            ReconcileLocker.LockKey lock1 = reconcileLocker.lockObject("container", "key", false);
            locks.add(lock1);
        }
        assertThatThrownBy(() -> reconcileLocker.lockObject("container", "key", false))
                .isInstanceOf(Error.class).hasMessage("Maximum lock count exceeded");
        locks.forEach(lock -> lock.close());
        assertThat(reconcileLocker.size()).isEqualTo(0);
    }
}
