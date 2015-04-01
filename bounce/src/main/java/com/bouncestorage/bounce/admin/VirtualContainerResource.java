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
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Joiner;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.builder.HashCodeBuilder;

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
        Configuration config = app.getConfiguration();
        String prefix = VIRTUAL_CONTAINER_PREFIX + "." + nextIndex;
        config.setProperty(Joiner.on(".").join(prefix, "name"), container.getName());
        Location origin = container.getOriginLocation();
        config.setProperty(Joiner.on(".").join(prefix, VirtualContainer.ORIGIN_LOCATION_PREFIX,
                Location.BLOB_STORE_ID_FIELD), Integer.toString(origin.getBlobStoreId()));
        config.setProperty(Joiner.on(".").join(prefix, VirtualContainer.ORIGIN_LOCATION_PREFIX,
                Location.CONTAINER_NAME_FIELD), origin.getContainerName());
        return "{\"status\":\"success\"}";
    }

    @Path("{id}")
    @PUT
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public String updateContainer(@PathParam("id") int id, VirtualContainer container) {
        Configuration config = app.getConfiguration();
        VirtualContainer current = getContainer(id, config);
        if (current == null) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        if (!current.getOriginLocation().equals(container.getOriginLocation())) {
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }

        String prefix = Joiner.on(".").join(VIRTUAL_CONTAINER_PREFIX, id);
        if (current.getArchiveLocation().isUnset()) {
            current.setArchiveLocation(container.getArchiveLocation());
            String locationPrefix = Joiner.on(".").join(prefix, VirtualContainer.ARCHIVE_LOCATION_PREFIX);
            updateLocationConfig(config, current.getArchiveLocation(), locationPrefix);
        }
        if (current.getCacheLocation().isUnset()) {
            current.setCacheLocation(container.getCacheLocation());
            String locationPrefix = Joiner.on(".").join(prefix, VirtualContainer.CACHE_LOCATION_PREFIX);
            updateLocationConfig(config, current.getCacheLocation(), locationPrefix);
        }
        if (current.getMigrationTargetLocation().isUnset()) {
            current.setMigrationTargetLocation(container.getMigrationTargetLocation());
            String locationPrefix = Joiner.on(".").join(prefix, VirtualContainer.MIGRATION_TARGET_LOCATION_PREFIX);
            updateLocationConfig(config, current.getMigrationTargetLocation(), locationPrefix);
        }

        config.setProperty(Joiner.on(".").join(prefix, "name"), container.getName());

        return "{\"status\":\"success\"}";
    }

    private void updateLocationConfig(Configuration config, Location location, String prefix) {
        config.setProperty(Joiner.on(".").join(prefix, Location.BLOB_STORE_ID_FIELD),
                Integer.toString(location.getBlobStoreId()));
        config.setProperty(Joiner.on(".").join(prefix, Location.CONTAINER_NAME_FIELD), location.getContainerName());
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

    private String getFieldName(String key) {
        // Skip the dot after the prefix and the dot after the index
        int fieldNameIndex = key.indexOf(".", VIRTUAL_CONTAINER_PREFIX.length() + 1) + 1;
        return key.substring(fieldNameIndex);
    }

    private String getLocationField(String key) {
        int index = key.indexOf(".");
        if (index < 0) {
            return null;
        }
        return key.substring(index + 1);
    }

    private void setProperty(VirtualContainer container, String field, String value) {
        if (field.equalsIgnoreCase("name")) {
            container.setName(value);
        }
        Location location = null;
        if (field.startsWith(VirtualContainer.ORIGIN_LOCATION_PREFIX)) {
            location = container.getOriginLocation();
        } else if (field.startsWith(VirtualContainer.CACHE_LOCATION_PREFIX)) {
            location = container.getCacheLocation();
        } else if (field.startsWith(VirtualContainer.ARCHIVE_LOCATION_PREFIX)) {
            location = container.getArchiveLocation();
        } else if (field.startsWith(VirtualContainer.MIGRATION_TARGET_LOCATION_PREFIX)) {
            location = container.getMigrationTargetLocation();
        }
        if (location != null) {
            location.setField(getLocationField(field), value);
        }
    }

    private static class Location {
        static final String BLOB_STORE_ID_FIELD = "blobStoreId";
        static final String CONTAINER_NAME_FIELD = "containerName";

        private int blobStoreId = -1;
        private String containerName;

        public void setBlobStoreId(int id) {
            blobStoreId = id;
        }

        public int getBlobStoreId() {
            return blobStoreId;
        }

        public void setContainerName(String name) {
            containerName = name;
        }

        public String getContainerName() {
            return containerName;
        }

        public void setLocation(Location location) {
            containerName = location.containerName;
            blobStoreId = location.blobStoreId;
        }

        @Override
        public String toString() {
            return "blobStoreId: " + blobStoreId + " container: " + containerName;
        }

        @Override
        public boolean equals(Object other) {
            return other != null && other.getClass() == Location.class
                    && containerName.equals(((Location) other).getContainerName())
                    && blobStoreId == ((Location) other).getBlobStoreId();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(5, 127).append(containerName).append(blobStoreId).toHashCode();
        }

        @JsonIgnore
        public boolean isUnset() {
            return containerName == null || blobStoreId < 0;
        }

        public void setField(String key, String value) {
            if (key.equals(BLOB_STORE_ID_FIELD)) {
                setBlobStoreId(Integer.decode(value));
            } else if (key.equals(CONTAINER_NAME_FIELD)) {
                setContainerName(value);
            }
        }
    }

    private static class VirtualContainer {
        static final String ORIGIN_LOCATION_PREFIX = "originLocation";
        static final String CACHE_LOCATION_PREFIX = "cacheLocation";
        static final String ARCHIVE_LOCATION_PREFIX = "archiveLocation";
        static final String MIGRATION_TARGET_LOCATION_PREFIX = "migrationTargetLocation";

        private String name;
        private int id;
        private Location originLocation;

        private Location cacheLocation;
        private Location archiveLocation;
        private Location migrationTargetLocation;

        public VirtualContainer() {
            originLocation = new Location();
            archiveLocation = new Location();
            migrationTargetLocation = new Location();
            cacheLocation = new Location();
        }

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

        public void setOriginLocation(Location location) {
            this.originLocation.setLocation(location);
        }

        public Location getOriginLocation() {
            return originLocation;
        }

        public String toString() {
            return "Name: " + name + " ID: " + id + " originLocation: " + originLocation.toString();
        }

        public Location getCacheLocation() {
            return cacheLocation;
        }

        public void setCacheLocation(Location cacheLocation) {
            this.cacheLocation = cacheLocation;
        }

        public Location getArchiveLocation() {
            return archiveLocation;
        }

        public void setArchiveLocation(Location archiveLocation) {
            this.archiveLocation = archiveLocation;
        }

        public Location getMigrationTargetLocation() {
            return migrationTargetLocation;
        }

        public void setMigrationTargetLocation(Location migrationTargetLocation) {
            this.migrationTargetLocation = migrationTargetLocation;
        }
    }
}
