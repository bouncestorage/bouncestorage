/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import com.google.inject.ImplementedBy;

import org.jclouds.blobstore.BlobStoreContext;

@ImplementedBy(EncryptedBlobStoreContextImpl.class)
public interface EncryptedBlobStoreContext extends BlobStoreContext {
}
