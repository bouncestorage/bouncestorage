/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import com.bouncestorage.bounce.admin.BounceApplication;
import com.bouncestorage.bounce.admin.ContainerPool;

import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;

public class BounceTestListener extends RunListener {
    @Override
    public void testRunStarted(Description description) {
        // Done to ensure that the BounceApplication class is loaded once
        new BounceApplication();
    }

    @Override
    public void testRunFinished(Result result) {
        ContainerPool.destroyAllContainers();
    }
}
