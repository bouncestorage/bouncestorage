/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.configuration.MapConfiguration;

public class BounceConfiguration extends MapConfiguration {
    public BounceConfiguration() {
        super(new HashMap<>());
    }

    public void setAll(Properties properties) {
        Set<Map.Entry<Object, Object>> newEntries = properties.entrySet();
        newEntries.forEach(entry ->
                fireEvent(EVENT_SET_PROPERTY, String.valueOf(entry.getKey()), entry.getValue(), true));
        setDetailEvents(false);
        try {
            newEntries.forEach(entry -> setProperty(String.valueOf(entry.getKey()), entry.getValue()));
        } finally {
            setDetailEvents(true);
        }
        newEntries.forEach(entry ->
                fireEvent(EVENT_SET_PROPERTY, String.valueOf(entry.getKey()), entry.getValue(), false));
    }
}
