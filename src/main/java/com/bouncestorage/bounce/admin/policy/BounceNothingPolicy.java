/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.admin.BouncePolicy;

public final class BounceNothingPolicy implements BouncePolicy {
    @Override
    public boolean test(Utils.ListBlobMetadata blobMetadata) {
        return false;
    }
}
