/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import com.bouncestorage.bounce.UtilsTest;

import org.junit.Before;
import org.junit.Test;

public final class AboutResourceTest {
    private AboutResource resource;

    @Before
    public void setup() throws Exception {
        UtilsTest.newBounceApplication();
        resource = new AboutResource();
    }

    @Test
    public void testBuildNumber() throws Exception {
        Properties p = resource.getBuildProperties();
        String sha1 = p.getProperty("git-sha-1");
        assertThat(sha1).hasSize(40);
    }
}
