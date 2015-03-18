/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import java.io.IOException;

import com.bouncestorage.bounce.BounceLink;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.options.GetOptions;

public abstract class MovePolicy extends MarkerPolicy {
    @Override
    public final Blob getBlob(String container, String blobName, GetOptions options) {
        BlobMetadata meta = getSource().blobMetadata(container, blobName);
        if (meta == null) {
            return null;
        }
        if (BounceLink.isLink(meta)) {
            Blob blob = getDestination().getBlob(container, blobName, options);
            try {
                Utils.copyBlob(getDestination(), getSource(), container, container, blobName);
            } catch (IOException e) {
                return blob;
            }
        }
        return getSource().getBlob(container, blobName, options);
    }
}
