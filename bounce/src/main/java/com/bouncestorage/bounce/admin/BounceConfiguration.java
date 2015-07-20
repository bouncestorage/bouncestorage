/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static com.google.common.base.Throwables.propagate;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class BounceConfiguration extends PropertiesConfiguration {
    private String fileName;

    public BounceConfiguration(String filename) throws ConfigurationException {
        super(filename);
        this.fileName = filename;
        Properties system = new Properties();
        for (Object k : System.getProperties().keySet()) {
            String s = k.toString();
            if (s.startsWith("bounce.")) {
                system.setProperty(s, System.getProperty(s));
            }
        }
        setAll(system);
    }

    public BounceConfiguration() {
        super();
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
        if (fileName != null) {
            try {
                save(fileName);
            } catch (ConfigurationException e) {
                propagate(e);
            }
        }
    }
}
