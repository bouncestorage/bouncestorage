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

import com.bouncestorage.bounce.admin.policy.LRUStoragePolicy;
import com.bouncestorage.bounce.admin.policy.MigrationPolicy;
import com.bouncestorage.bounce.admin.policy.StoragePolicy;
import com.bouncestorage.bounce.admin.policy.WriteBackPolicy;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Joiner;

import org.apache.commons.configuration.Configuration;

@Path("/virtual_container")
@Produces(MediaType.APPLICATION_JSON)
public final class VirtualContainerResource {
    public static final String VIRTUAL_CONTAINER_PREFIX = "bounce.container";
    public static final String CONTAINERS_PREFIX = "bounce.containers";

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
        container.setId(getNextContainerId());
        BounceConfiguration config = app.getConfiguration();
        Properties newProperties;
        try {
            newProperties = generatePropertiesFromRequest(container, null);
        } catch (IllegalArgumentException e) {
            throw new WebApplicationException(e, Response.Status.BAD_REQUEST);
        }
        if (config.getProperty(CONTAINERS_PREFIX) == null) {
            newProperties.setProperty(CONTAINERS_PREFIX, Integer.toString(container.getId()));
        } else {
            newProperties.setProperty(CONTAINERS_PREFIX, config.getString(CONTAINERS_PREFIX) + "," +
                    Integer.toString(container.getId()));
        }
        config.setAll(newProperties);
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
        try {
            config.setAll(generatePropertiesFromRequest(container, current));
        } catch (IllegalArgumentException e) {
            throw new WebApplicationException(e, Response.Status.BAD_REQUEST);
        }

        return "{\"status\":\"success\"}";
    }

    private Properties generatePropertiesFromRequest(VirtualContainer request, VirtualContainer current) {
        Properties properties = new Properties();
        String prefix = Joiner.on(".").join(VIRTUAL_CONTAINER_PREFIX, request.getId());
        properties.setProperty(Joiner.on(".").join(prefix, VirtualContainer.NAME), request.getName());
        if (current == null || current.getOriginLocation().permittedChange(request.getOriginLocation())) {
            String locationPrefix = Joiner.on(".").join(prefix, VirtualContainer.PRIMARY_TIER_PREFIX);
            updateLocationConfig(properties, request.getOriginLocation(), locationPrefix);
        }
        if (current == null || current.getArchiveLocation().permittedChange(request.getArchiveLocation())) {
            String locationPrefix = Joiner.on(".").join(prefix, VirtualContainer.ARCHIVE_TIER_PREFIX);
            updateLocationConfig(properties, request.getArchiveLocation(), locationPrefix);
            if (isPolicyUnset(current, request, VirtualContainer.TIER.ARCHIVE)) {
                if (request.getOriginLocation().getCapacity() != null &&
                        request.getOriginLocation().getCapacity() > 0) {
                    properties.setProperty(Joiner.on(".").join(prefix, VirtualContainer.PRIMARY_TIER_PREFIX, "policy"),
                            LRUStoragePolicy.class.getSimpleName());
                } else {
                    properties.setProperty(Joiner.on(".").join(prefix, VirtualContainer.PRIMARY_TIER_PREFIX, "policy"),
                            WriteBackPolicy.class.getSimpleName());
                }
            }
        }
        if (current == null || current.getCacheLocation().permittedChange(request.getCacheLocation())) {
            String locationPrefix = Joiner.on(".").join(prefix, VirtualContainer.CACHE_TIER_PREFIX);
            updateLocationConfig(properties, request.getCacheLocation(), locationPrefix);
            if (isPolicyUnset(current, request, VirtualContainer.TIER.CACHE)) {
                if (request.getCacheLocation().getCapacity() != null) {
                    properties.setProperty(Joiner.on(".").join(prefix, VirtualContainer.CACHE_TIER_PREFIX, "policy"),
                            LRUStoragePolicy.class.getSimpleName());
                } else {
                    properties.setProperty(Joiner.on(".").join(prefix, VirtualContainer.CACHE_TIER_PREFIX, "policy"),
                            WriteBackPolicy.class.getSimpleName());
                }
            }
        }
        if (current == null || current.getMigrationTargetLocation().permittedChange(
                request.getMigrationTargetLocation())) {
            String locationPrefix = Joiner.on(".").join(prefix, VirtualContainer.MIGRATION_TIER_PREFIX);
            updateLocationConfig(properties, request.getMigrationTargetLocation(), locationPrefix);
            if (isPolicyUnset(current, request, VirtualContainer.TIER.MIGRATION)) {
                properties.setProperty(Joiner.on(".").join(prefix, VirtualContainer.PRIMARY_TIER_PREFIX, "policy"),
                        MigrationPolicy.class.getSimpleName());
            }
        }

        properties.setProperty(Joiner.on(".").join(prefix, VirtualContainer.NAME), request.getName());
        return properties;
    }

    private boolean isPolicyUnset(VirtualContainer current, VirtualContainer newContainer, VirtualContainer.TIER tier) {
        requireNonNull(newContainer);
        Location currentLocation = null;
        Location newLocation;
        switch (tier) {
            case ARCHIVE:
                if (current != null) {
                    currentLocation = current.getArchiveLocation();
                }
                newLocation = newContainer.getArchiveLocation();
                break;
            case CACHE:
                if (current != null) {
                    currentLocation = current.getCacheLocation();
                }
                newLocation = newContainer.getCacheLocation();
                break;
            case MIGRATION:
                if (current != null) {
                    currentLocation = current.getMigrationTargetLocation();
                }
                newLocation = newContainer.getMigrationTargetLocation();
                break;
            default:
                throw new IllegalArgumentException("Unknown tier");
        }
        return (currentLocation == null || currentLocation.isUnset()) && !newLocation.isUnset();
    }

    private void updateLocationConfig(Properties properties, Location location, String prefix) {
        if (location.isUnset()) {
            return;
        }
        if (location.getCapacity() != null && location.getCapacity() > 0) {
            if (location.getMoveDelay() != null && !location.getMoveDelay().startsWith("-")) {
                // Cannot configure an expiration or move policy, while also defining a storage policy
                throw new IllegalArgumentException("Cannot set both capacity and delay");
            }
        }
        properties.setProperty(Joiner.on(".").join(prefix, Location.BLOB_STORE_ID_FIELD),
                Integer.toString(location.getBlobStoreId()));
        properties.setProperty(Joiner.on(".").join(prefix, Location.CONTAINER_NAME_FIELD), location.getContainerName());
        if (location.getCopyDelay() != null) {
            properties.setProperty(Joiner.on(".").join(prefix, WriteBackPolicy.COPY_DELAY), location.getCopyDelay());
        }
        if (location.getMoveDelay() != null) {
            properties.setProperty(Joiner.on(".").join(prefix, WriteBackPolicy.EVICT_DELAY), location.getMoveDelay());
        }
        if (location.getCapacity() != null && location.getCapacity() > 0) {
            properties.setProperty(Joiner.on(".").join(prefix, StoragePolicy.CAPACITY_SETTING),
                    location.getCapacity().toString());
        }
    }

    private VirtualContainer getContainer(int id, Configuration config) {
        Iterator<String> keyIterator = app.getConfiguration().getKeys(VIRTUAL_CONTAINER_PREFIX + "." + id);
        VirtualContainer result = null;
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            if (result == null) {
                result = new VirtualContainer();
                result.setId(id);
            }
            setProperty(result, getFieldName(key), config.getString(key));
        }
        return result;
    }

    private int getContainerId(String key) {
        int indexStart = VIRTUAL_CONTAINER_PREFIX.length() + 1;
        int indexEnd = key.indexOf(".", indexStart);
        if (indexEnd < 0) {
            return -1;
        }
        String indexString = key.substring(indexStart, indexEnd);
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
            if (containerId > 0 && containerId >= nextIndex) {
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
        if (field.startsWith(VirtualContainer.NAME)) {
            container.setName(value);
            return;
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
