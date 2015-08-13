/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.utils;

import java.util.Map;

import com.bouncestorage.bounce.admin.BounceApplication;

import org.jclouds.blobstore.BlobStore;

import autovalue.shaded.com.google.common.common.collect.Maps;

public class BounceAutoConfigApplication extends BounceApplication {
    private AutoConfigBlobStore autoConfigBlobStore;

    public BounceAutoConfigApplication(String configFilePath) {
        super(configFilePath);
        autoConfigBlobStore = new AutoConfigBlobStore(this);
    }

    @Override
    public Map.Entry<String, BlobStore> locateBlobStore(String identity, String container, String blob) {
        if (identity.equals("test2:tester2")) {
            return Maps.immutableEntry("testing2", autoConfigBlobStore);
        } else {
            return Maps.immutableEntry("testing", autoConfigBlobStore);
        }
    }

    @Override
    public BlobStore getBlobStore(String container) {
        return autoConfigBlobStore.getPolicyFromContainer(container);
    }
}
