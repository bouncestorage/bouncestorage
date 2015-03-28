/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;

import org.apache.commons.configuration.Configuration;

@Path("/virtual_container")
@Produces(MediaType.APPLICATION_JSON)
public final class VirtualContainerResource {
    public static final String VIRTUAL_CONTAINER_PREFIX = "bounce.virtualContainer";

    private final BounceApplication app;

    public VirtualContainerResource(BounceApplication app) {
        this.app = requireNonNull(app);
    }

    @GET
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public List<VirtualContainer> getContainers() {
        Configuration config = app.getConfiguration();
        Iterator<String> iterator = config.getKeys();
        Map<Integer, VirtualContainer> results = new HashMap<>();
        while (iterator.hasNext()) {
            String key = iterator.next();
            if (!key.startsWith(VIRTUAL_CONTAINER_PREFIX)) {
                continue;
            }
            int id = getContainerId(key);
            VirtualContainer container;
            if (!results.containsKey(id)) {
                container = new VirtualContainer();
                container.setId(id);
                results.put(id, container);
            } else {
                container = results.get(id);
            }
            // Skip the dot after the prefix and the dot after the index
            int fieldNameIndex = key.indexOf(".", VIRTUAL_CONTAINER_PREFIX.length() + 1) + 1;
            String fieldName = key.substring(fieldNameIndex);
            setProperty(container, fieldName, config.getString(key));
        }

        return new LinkedList<>(results.values());
    }

    @POST
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String createContainer(VirtualContainer container) {
        int nextIndex = getNextContainerId();
        Configuration config = app.getConfiguration();
        String prefix = VIRTUAL_CONTAINER_PREFIX + "." + nextIndex + ".";
        config.setProperty(prefix + "name", container.getName());
        config.setProperty(prefix + "originBlobStoreId", Integer.toString(container.getOriginBlobStoreId()));
        config.setProperty(prefix + "originContainerName", container.getOriginContainerName());
        return "{\"status\":\"success\"}";
    }

    private int getContainerId(String key) {
        int indexStart = VIRTUAL_CONTAINER_PREFIX.length() + 1;
        String indexString = key.substring(indexStart, key.indexOf(".", indexStart));
        return Integer.decode(indexString);
    }

    private int getNextContainerId() {
        Configuration config = app.getConfiguration();
        Iterator<String> iterator = config.getKeys();
        int nextIndex = 1;
        while (iterator.hasNext()) {
            String key = iterator.next();
            if (!key.startsWith(VIRTUAL_CONTAINER_PREFIX)) {
                continue;
            }
            int containerId = getContainerId(key);
            if (containerId >= nextIndex) {
                nextIndex = containerId + 1;
            }
        }
        return nextIndex;
    }

    private void setProperty(VirtualContainer container, String field, String value) {
        if (field.equalsIgnoreCase("name")) {
            container.setName(value);
        } else if (field.equalsIgnoreCase("originBlobStoreId")) {
            container.setOriginBlobStoreId(Integer.decode(value));
        } else if (field.equalsIgnoreCase("originContainerName")) {
            container.setOriginContainerName(value);
        }
    }

    private static class VirtualContainer {
        private String name;
        private int id;
        private int originBlobStoreId;
        private String originContainerName;

        public void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setId(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        public void setOriginBlobStoreId(int originBlobStoreId) {
            this.originBlobStoreId = originBlobStoreId;
        }

        public int getOriginBlobStoreId() {
            return originBlobStoreId;
        }

        public void setOriginContainerName(String originContainerName) {
            this.originContainerName = originContainerName;
        }

        public String getOriginContainerName() {
            return originContainerName;
        }

        public String toString() {
            return "Name: " + name + " ID: " + id + " originBlobStoreId: " + originBlobStoreId + " " +
                    "originContainerName: " + originContainerName;
        }
    }
}
