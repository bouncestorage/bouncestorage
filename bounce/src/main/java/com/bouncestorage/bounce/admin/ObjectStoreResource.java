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
import java.util.TreeMap;
import java.util.stream.Collectors;

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

import com.bouncestorage.bounce.BounceBlobStore;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.configuration.Configuration;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;

@Path("/object_store")
@Produces(MediaType.APPLICATION_JSON)
public final class ObjectStoreResource {
    private static final String SUCCESS_RESPONSE = "{\"status\": \"success\"}";
    private static final String PROPERTIES_PREFIX = "jclouds.";
    private static final ImmutableSet<String> IMMUTABLE_FIELDS = ImmutableSet.of("identity", "provider",
            "region", "endpoint");

    private final BounceApplication app;

    public ObjectStoreResource(BounceApplication app) {
        this.app = requireNonNull(app);
    }

    @POST
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public String createObjectStore(ObjectStore objectStore) {
        try {
            Properties properties = objectStore.getJCloudsProperties(PROPERTIES_PREFIX);
            ContextBuilder builder = ContextBuilder.newBuilder(objectStore.provider).overrides(properties);
            BlobStoreContext context = builder.build(BlobStoreContext.class);
            BlobStore store = context.getBlobStore();
            store.list();
            BounceConfiguration config = app.getConfiguration();
            int storeIndex = getLastBlobStoreIndex(config);
            String prefix = BounceBlobStore.STORE_PROPERTY + "." + storeIndex + "." + PROPERTIES_PREFIX;
            properties = objectStore.getJCloudsProperties(prefix);
            List<Object> backendsList = config.getList(BounceBlobStore.STORES_LIST);
            if (backendsList == null || backendsList.isEmpty()) {
                properties.setProperty(BounceBlobStore.STORES_LIST, Integer.toString(storeIndex));
            } else {
                backendsList.add(Integer.toString(storeIndex));
                properties.setProperty(BounceBlobStore.STORES_LIST, Joiner.on(",").join(backendsList));
            }
            config.setAll(properties);
            return SUCCESS_RESPONSE;
        } catch (Exception e) {
            e.printStackTrace();
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("{id}")
    @Timed
    public ObjectStore getObjectStore(@PathParam("id") int id) {
        ObjectStore store = getStoreById(id, app.getConfiguration());
        if (store == null) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        return store;
    }

    @POST
    @Path("{id}/container")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createContainer(@PathParam("id") int providerId, HashMap<String, String> request) {
        BlobStore blobStore = app.getBlobStore(providerId);
        if (blobStore == null) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        if (!request.containsKey("name")) {
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }
        blobStore.createContainerInLocation(null, request.get("name"));
        return Response.ok().build();
    }

    @GET
    @Path("{id}/container")
    @Timed
    public List<Container> getContainerList(@PathParam("id") int providerId) {
        BlobStore blobStore = app.getBlobStore(providerId);
        if (blobStore == null) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        PageSet<? extends StorageMetadata> pageSet = blobStore.list();
        List<VirtualContainer> vContainers = new VirtualContainerResource(app).getContainers();
        Map<String, ContainerMapEntry> containerMap = new TreeMap<>();
        for (VirtualContainer container : vContainers) {
            Location[] locations = {container.getCacheLocation(), container.getArchiveLocation(), container
                    .getMigrationTargetLocation()};
            for (Location location : locations) {
                if (location != null && location.getBlobStoreId() == providerId) {
                    ContainerMapEntry entry = new ContainerMapEntry();
                    entry.status = Container.ContainerStatus.INUSE;
                    entry.virtualContainerId = container.getId();
                    containerMap.put(location.getContainerName(), entry);
                }
            }
            if (container.getOriginLocation().getBlobStoreId() == providerId) {
                ContainerMapEntry entry = new ContainerMapEntry();
                entry.status = Container.ContainerStatus.CONFIGURED;
                entry.virtualContainerId = container.getId();
                containerMap.put(container.getOriginLocation().getContainerName(), entry);
            }
        }
        return pageSet.stream()
                .map(sm -> {
                    Container container = new Container(sm.getName());
                    if (containerMap.containsKey(container.getName())) {
                        ContainerMapEntry entry = containerMap.get(container.getName());
                        container.setStatus(entry.status);
                        container.setVirtualContainerId(entry.virtualContainerId);
                    }
                    return container;
                })
                .collect(Collectors.toList());
    }

    @GET
    @Timed
    public List<ObjectStore> getObjectStores() {
        Map<Integer, ObjectStore> results = new HashMap<>();
        Configuration config = app.getConfiguration();
        Iterator<String> configIterator = config.getKeys(BounceBlobStore.STORE_PROPERTY);
        while (configIterator.hasNext()) {
            String key = configIterator.next();
            int id = getStoreId(key);
            ObjectStore store;
            if (!results.containsKey(id)) {
                store = new ObjectStore();
                store.setId(id);
                results.put(id, store);
            } else {
                store = results.get(id);
            }
            setProperty(store, getFieldName(key), config.getString(key));
        }
        return new LinkedList<>(results.values());
    }

    @PUT
    @Path("{id}")
    @Timed
    public String updateObjectStore(@PathParam("id") int id,
                                    ObjectStore objectStore) {
        BounceConfiguration config = app.getConfiguration();
        ObjectStore current = getStoreById(id, config);
        if (current == null) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        for (String field : IMMUTABLE_FIELDS) {
            String currentValue = current.getValueByName(field);
            String newValue = objectStore.getValueByName(field);
            boolean immutableSet = false;
            if (currentValue == null && newValue != null) {
                immutableSet = true;
            }
            if (currentValue != null && newValue != null) {
                if (!current.getValueByName(field).equalsIgnoreCase(objectStore.getValueByName(field))) {
                    immutableSet = true;
                }
            }
            if (immutableSet) {
                throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST).entity(
                    "Parameter " + field + " cannot be changed").build());
            }
        }

        String prefix = BounceBlobStore.STORE_PROPERTY + "." + current.id + ".";
        Properties properties = new Properties();
        properties.setProperty(prefix + "nickname", objectStore.nickname);
        properties.setProperty(prefix + PROPERTIES_PREFIX + "credential",
                objectStore.credential == null ? "" : objectStore.credential);
        config.setAll(properties);
        return "{\"status\":\"success\"}";
    }

    private ObjectStore getStoreById(int id, Configuration config) {
        String prefix = BounceBlobStore.STORE_PROPERTY + "." + id;
        Iterator<String> keysIterator = config.getKeys(prefix);
        ObjectStore result = null;
        while (keysIterator.hasNext()) {
            String key = keysIterator.next();
            if (result == null) {
                result = new ObjectStore();
                result.setId(id);
            }
            setProperty(result, getFieldName(key), config.getString(key));
        }
        return result;
    }

    private int getStoreId(String key) {
        int indexEnd = key.indexOf(".", BounceBlobStore.STORE_PROPERTY.length() + 1);
        return Integer.parseInt(key.substring(BounceBlobStore.STORE_PROPERTY.length() + 1, indexEnd));
    }

    private String getFieldName(String key) {
        int indexEnd;
        if (!key.contains(PROPERTIES_PREFIX)) {
            indexEnd = key.indexOf(".", BounceBlobStore.STORE_PROPERTY.length() + 1) + 1;
        } else {
            indexEnd = key.indexOf(PROPERTIES_PREFIX) + PROPERTIES_PREFIX.length();
        }
        return key.substring(indexEnd);
    }

    private void setProperty(ObjectStore store, String field, String value) {
        if (field.equalsIgnoreCase("provider")) {
            store.setProvider(value);
        } else if (field.equalsIgnoreCase("identity")) {
            store.setIdentity(value);
        } else if (field.equalsIgnoreCase("credential")) {
            store.setCredential(value);
        } else if (field.equalsIgnoreCase("nickname")) {
            store.setNickname(value);
        }
    }

    private int getLastBlobStoreIndex(Configuration config) {
        int lastIndex = 1;
        Iterator<String> keyIterator = config.getKeys(BounceBlobStore.STORE_PROPERTY);
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            // Skip the "." and the first digit
            int indexStart = key.indexOf(BounceBlobStore.STORE_PROPERTY) + BounceBlobStore.STORE_PROPERTY.length() + 1;
            int indexEnd = key.indexOf(".", indexStart);
            String indexString = key.substring(indexStart, indexEnd);
            int index = Integer.parseInt(indexString);
            if (index >= lastIndex) {
                lastIndex++;
            }
        }

        return lastIndex;
    }

    private static class ObjectStore {
        private String identity;
        private String credential;
        private String endpoint;
        private String nickname;
        private String provider;
        private String region;
        private int id;

        public void setIdentity(String identity) {
            this.identity = identity;
        }

        public String getIdentity() {
            return identity;
        }

        public void setCredential(String credential) {
            this.credential = credential;
        }

        public void setEndpoint(String endpoint) {
            this.endpoint = endpoint;
        }

        public String getEndpoint() {
            return endpoint;
        }

        public void setNickname(String nickname) {
            this.nickname = nickname;
        }

        public String getNickname() {
            return nickname;
        }

        public void setProvider(String provider) {
            this.provider = provider;
        }

        public String getProvider() {
            return provider;
        }

        public void setRegion(String region) {
            this.region = region;
        }

        public String getRegion() {
            return region;
        }

        public void setId(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        Properties getJCloudsProperties(String propertiesPrefix) {
            Properties properties = new Properties();

            properties.put(propertiesPrefix + "provider", provider);
            properties.put(propertiesPrefix + "identity", identity);
            properties.put(propertiesPrefix + "credential", credential);
            if (endpoint != null) {
                properties.put(propertiesPrefix + "endpoint", endpoint);
                if (endpoint.endsWith("/auth/v1.0/")) {
                    properties.put(propertiesPrefix + "keystone.credential-type", "tempAuthCredentials");
                }
            }
            if (region != null) {
                properties.put(propertiesPrefix + "region", region);
            }
            properties.put(propertiesPrefix + "nickname", nickname);

            return properties;
        }

        public String getValueByName(String field) {
            if (field.equalsIgnoreCase("nickname")) {
                return nickname;
            }
            if (field.equalsIgnoreCase("identity")) {
                return identity;
            }
            if (field.equalsIgnoreCase("credential")) {
                return credential;
            }
            if (field.equalsIgnoreCase("provider")) {
                return provider;
            }
            if (field.equalsIgnoreCase("region")) {
                return region;
            }
            if (field.equalsIgnoreCase("endpoint")) {
                return endpoint;
            }
            throw new IllegalArgumentException("Unknown field");
        }

        public String toString() {
            return "Id: " + id + " Provider: " + provider + " Identity: " + identity + " Credential: " + credential +
                    " Nickname: " + nickname;
        }
    }

    private static class ContainerMapEntry {
        private int virtualContainerId;
        private Container.ContainerStatus status;
    }

    private static class Container {
        enum ContainerStatus { UNCONFIGURED, CONFIGURED, INUSE };

        private String name;
        private ContainerStatus status;
        private int virtualContainerId;

        Container(String name) {
            this.name = name;
            virtualContainerId = -1;
            status = ContainerStatus.UNCONFIGURED;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setStatus(ContainerStatus status) {
            this.status = status;
        }

        public ContainerStatus getStatus() {
            return status;
        }

        public int getVirtualContainerId() {
            return virtualContainerId;
        }

        public void setVirtualContainerId(int virtualContainerId) {
            this.virtualContainerId = virtualContainerId;
        }
    }
}
