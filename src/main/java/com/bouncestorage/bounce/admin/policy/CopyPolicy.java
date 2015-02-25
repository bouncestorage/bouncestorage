/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import java.io.IOException;
import java.util.Map;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.bouncestorage.bounce.admin.BounceService;

import org.apache.commons.configuration.Configuration;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageMetadata;

public final class CopyPolicy implements BouncePolicy {
    public static final String COPIED_METADATA_KEY = "bounce-copied";

    private BounceService service;

    @Override
    public void init(BounceService bounceService, Configuration settings) {
        service = bounceService;
    }

    @Override
    public boolean test(StorageMetadata metadata) {
        return true;
    }

    @Override
    public BounceResult bounce(BlobMetadata blobMetadata, BounceBlobStore blobStore) throws IOException {
        if (BounceLink.isLink(blobMetadata)) {
            return BounceResult.NO_OP;
        }

        if (blobMetadata.getUserMetadata().containsKey(COPIED_METADATA_KEY)) {
            return BounceResult.NO_OP;
        }
        blobStore.copyBlob(blobMetadata.getContainer(), blobMetadata.getName());
        Blob blob = blobStore.getBlob(blobMetadata.getContainer(), blobMetadata.getName());
        Map<String, String> userMetadata = blobMetadata.getUserMetadata();
        userMetadata.put(COPIED_METADATA_KEY, "true");
        blob.getMetadata().setUserMetadata(userMetadata);
        blobStore.putBlob(blobMetadata.getContainer(), blob);
        return BounceResult.COPY;
    }
}
