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
import java.util.Properties;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Joiner;

import org.apache.commons.configuration.Configuration;

@Path("/virtual_container")
@Produces(MediaType.APPLICATION_JSON)
public final class VirtualContainerResource {
    public static final String VIRTUAL_CONTAINER_PREFIX = "bounce.container";

    private final BounceApplication app;

    public VirtualContainerResource(BounceApplication app) {
        this.app = requireNonNull(app);
    }

    @GET
    @Timed
    public List<VirtualContainer> getContainers() {
        Configuration config = app.getConfiguration();
        Iterator<String> iterator = config.getKeys(VIRTUAL_CONTAINER_PREFIX);
        Map<Integer, VirtualContainer> results = new HashMap<>();
        while (iterator.hasNext()) {
            String key = iterator.next();
            int id = getContainerId(key);
            VirtualContainer container;
            if (!results.containsKey(id)) {
                container = new VirtualContainer();
                container.setId(id);
                results.put(id, container);
            } else {
                container = results.get(id);
            }
            setProperty(container, getFieldName(key), config.getString(key));
        }

        return new LinkedList<>(results.values());
    }

    @Path("{id}")
    @GET
    @Timed
    public VirtualContainer getContainer(@PathParam("id") int id) {
        VirtualContainer container = getContainer(id, app.getConfiguration());
        if (container == null) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        return container;
    }

    @POST
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public String createContainer(VirtualContainer container) {
        int nextIndex = getNextContainerId();
        BounceConfiguration config = app.getConfiguration();
        String prefix = VIRTUAL_CONTAINER_PREFIX + "." + nextIndex;
        Properties properties = new Properties();
        properties.setProperty(Joiner.on(".").join(prefix, VirtualContainer.NAME), container.getName());
        Location origin = container.getOriginLocation();
        properties.setProperty(Joiner.on(".").join(prefix, VirtualContainer.PRIMARY_TIER_PREFIX,
                Location.BLOB_STORE_ID_FIELD), Integer.toString(origin.getBlobStoreId()));
        properties.setProperty(Joiner.on(".").join(prefix, VirtualContainer.PRIMARY_TIER_PREFIX,
                Location.CONTAINER_NAME_FIELD), origin.getContainerName());
        config.setAll(properties);
        return "{\"status\":\"success\"}";
    }

    @Path("{id}")
    @PUT
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public String updateContainer(@PathParam("id") int id, VirtualContainer container) {
        BounceConfiguration config = app.getConfiguration();
        VirtualContainer current = getContainer(id, config);
        if (current == null) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        if (!current.getOriginLocation().equals(container.getOriginLocation())) {
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }

        Properties properties = new Properties();
        String prefix = Joiner.on(".").join(VIRTUAL_CONTAINER_PREFIX, id);
        if (current.getArchiveLocation().isUnset()) {
            current.setArchiveLocation(container.getArchiveLocation());
            String locationPrefix = Joiner.on(".").join(prefix, VirtualContainer.ARCHIVE_TIER_PREFIX);
            updateLocationConfig(properties, current.getArchiveLocation(), locationPrefix);
        }
        if (current.getCacheLocation().isUnset()) {
            current.setCacheLocation(container.getCacheLocation());
            String locationPrefix = Joiner.on(".").join(prefix, VirtualContainer.CACHE_TIER_PREFIX);
            updateLocationConfig(properties, current.getCacheLocation(), locationPrefix);
        }
        if (current.getMigrationTargetLocation().isUnset()) {
            current.setMigrationTargetLocation(container.getMigrationTargetLocation());
            String locationPrefix = Joiner.on(".").join(prefix, VirtualContainer.MIGRATION_TIER_PREFIX);
            updateLocationConfig(properties, current.getMigrationTargetLocation(), locationPrefix);
        }

        properties.setProperty(Joiner.on(".").join(prefix, VirtualContainer.NAME), container.getName());
        config.setAll(properties);

        return "{\"status\":\"success\"}";
    }

    private void updateLocationConfig(Properties properties, Location location, String prefix) {
        properties.setProperty(Joiner.on(".").join(prefix, Location.BLOB_STORE_ID_FIELD),
                Integer.toString(location.getBlobStoreId()));
        properties.setProperty(Joiner.on(".").join(prefix, Location.CONTAINER_NAME_FIELD), location.getContainerName());
    }

    private VirtualContainer getContainer(int id, Configuration config) {
        Iterator<String> keyIterator = app.getConfiguration().getKeys(VIRTUAL_CONTAINER_PREFIX + "." + id);
        VirtualContainer result = null;
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            if (result == null) {
                result = new VirtualContainer();
            }
            setProperty(result, getFieldName(key), config.getString(key));
        }
        return result;
    }

    private int getContainerId(String key) {
        int indexStart = VIRTUAL_CONTAINER_PREFIX.length() + 1;
        String indexString = key.substring(indexStart, key.indexOf(".", indexStart));
        return Integer.parseInt(indexString);
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

    private String getFieldName(String key) {
        // Skip the dot after the prefix and the dot after the index
        int fieldNameIndex = key.indexOf(".", VIRTUAL_CONTAINER_PREFIX.length() + 1) + 1;
        return key.substring(fieldNameIndex);
    }

    private String getLocationField(String key) {
        int index = key.indexOf(".", VirtualContainer.TIER_PREFIX.length() + 1);
        if (index < 0) {
            return null;
        }
        return key.substring(index + 1);
    }

    private void setProperty(VirtualContainer container, String field, String value) {
        if (field.equalsIgnoreCase(VirtualContainer.NAME)) {
            container.setName(value);
        }
        Location location = null;
        if (field.startsWith(VirtualContainer.PRIMARY_TIER_PREFIX)) {
            location = container.getOriginLocation();
        } else if (field.startsWith(VirtualContainer.CACHE_TIER_PREFIX)) {
            location = container.getCacheLocation();
        } else if (field.startsWith(VirtualContainer.ARCHIVE_TIER_PREFIX)) {
            location = container.getArchiveLocation();
        } else if (field.startsWith(VirtualContainer.MIGRATION_TIER_PREFIX)) {
            location = container.getMigrationTargetLocation();
        }
        if (location != null) {
            location.setField(getLocationField(field), value);
        }
    }
}
