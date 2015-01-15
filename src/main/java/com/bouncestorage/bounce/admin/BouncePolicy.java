/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import java.util.Properties;
import java.util.function.Predicate;

import org.jclouds.blobstore.domain.StorageMetadata;

public interface BouncePolicy extends Predicate<StorageMetadata> {
    default void init(BounceService service, Properties properties) {
    }
}
