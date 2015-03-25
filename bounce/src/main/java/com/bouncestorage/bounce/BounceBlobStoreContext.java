/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import com.google.inject.ImplementedBy;

import org.jclouds.blobstore.BlobStoreContext;

/**
 * Created by khc on 1/2/15.
 */
@ImplementedBy(BounceBlobStoreContextImpl.class)
public interface BounceBlobStoreContext extends BlobStoreContext {
}
