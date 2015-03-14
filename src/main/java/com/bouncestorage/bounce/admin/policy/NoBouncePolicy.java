/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import com.bouncestorage.bounce.admin.BouncePolicy;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.PutOptions;

public abstract class NoBouncePolicy extends BouncePolicy {
    @Override
    public final String putBlob(String container, Blob blob, PutOptions options) {
        return getSource().putBlob(container, blob, options);
    }

    @Override
    public final Blob getBlob(String container, String blobName, GetOptions options) {
        return getSource().getBlob(container, blobName, options);
    }
}
