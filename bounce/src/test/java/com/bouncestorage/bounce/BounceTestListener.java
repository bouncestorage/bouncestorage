/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import com.bouncestorage.bounce.admin.ContainerPool;

import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;

public class BounceTestListener extends RunListener {
    @Override
    public void testRunFinished(Result result) {
        ContainerPool.destroyAllContainers();
    }
}
