/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import com.bouncestorage.bounce.admin.BouncePolicy;
import com.google.auto.service.AutoService;

@AutoService(BouncePolicy.class)
public final class BounceNothingPolicy extends NoBouncePolicy {
}
