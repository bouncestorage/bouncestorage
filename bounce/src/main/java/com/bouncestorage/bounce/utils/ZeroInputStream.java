/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.utils;

import java.io.IOException;
import java.io.InputStream;

public class ZeroInputStream extends InputStream {
    @Override
    public int read() throws IOException {
        return 0;
    }
}
