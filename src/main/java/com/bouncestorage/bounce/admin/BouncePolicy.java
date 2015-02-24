/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import java.io.IOException;
import java.util.function.Predicate;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.Utils;

import org.apache.commons.configuration.Configuration;

public interface BouncePolicy extends Predicate<Utils.ListBlobMetadata> {
    default void init(BounceService service, Configuration config) {
    }

    default void bounce(String container, String blobName, BounceBlobStore blobStore) throws IOException {
    }
}
