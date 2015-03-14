/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import com.bouncestorage.bounce.admin.BouncePolicy;
import com.google.common.io.ByteSource;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.options.PutOptions;

public abstract class MarkerPolicy implements BouncePolicy {
    public static final String LOG_MARKER_SUFFIX = "%01log";

    public static boolean isMarkerBlob(String name) {
        return name.endsWith(LOG_MARKER_SUFFIX);
    }

    public static String markerBlobGetName(String marker) {
        return marker.substring(0, marker.length() - LOG_MARKER_SUFFIX.length());
    }

    public final String putBlob(String container, Blob blob, PutOptions options) {
        putMarkerBlob(container, blob.getMetadata().getName());
        return getSource().putBlob(container, blob, options);
    }

    private void putMarkerBlob(String containerName, String key) {
        getSource().putBlob(containerName,
                getSource().blobBuilder(key + MarkerPolicy.LOG_MARKER_SUFFIX).payload(ByteSource.empty()).build());
    }
}
