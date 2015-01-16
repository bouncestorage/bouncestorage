/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import io.dropwizard.Configuration;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.server.DefaultServerFactory;

public final class BounceConfiguration extends Configuration {
    void useRandomPorts() {
        DefaultServerFactory serverFactory = (DefaultServerFactory) getServerFactory();
        ((HttpConnectorFactory) serverFactory.getApplicationConnectors().get(0)).setPort(0);
        ((HttpConnectorFactory) serverFactory.getAdminConnectors().get(0)).setPort(0);
    }
}
