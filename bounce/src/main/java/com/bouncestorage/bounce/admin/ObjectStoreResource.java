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
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.bouncestorage.bounce.BounceBlobStore;
import com.codahale.metrics.annotation.Timed;

import org.apache.commons.configuration.Configuration;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;

@Path("/object_store")
@Produces(MediaType.APPLICATION_JSON)
public final class ObjectStoreResource {
    private static final String SUCCESS_RESPONSE = "{\"status\": \"success\"}";
    private static final String PROPERTIES_PREFIX = "jclouds.";

    private final BounceApplication app;

    public ObjectStoreResource(BounceApplication app) {
        this.app = requireNonNull(app);
    }

    @POST
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String createObjectStore(ObjectStore objectStore) {
        try {
            Properties properties = new Properties();
            properties.put(PROPERTIES_PREFIX + "identity", objectStore.identity);
            properties.put(PROPERTIES_PREFIX + "credential", objectStore.credential);
            ContextBuilder builder = ContextBuilder.newBuilder(objectStore.provider).overrides(properties);
            BlobStoreContext context = builder.build(BlobStoreContext.class);
            BlobStore store = context.getBlobStore();
            store.list();
            Configuration config = app.getConfiguration();
            int storeIndex = getLastBlobStoreIndex(config);
            String prefix = BounceBlobStore.STORE_PROPERTY + "." + storeIndex + "." + PROPERTIES_PREFIX;
            config.setProperty(prefix + "provider", objectStore.provider);
            config.setProperty(prefix + "identity", objectStore.identity);
            config.setProperty(prefix + "credential", objectStore.credential);
            config.setProperty(prefix + "nickname", objectStore.nickname);
            return SUCCESS_RESPONSE;
        } catch (Exception e) {
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public List<ObjectStore> getObjectStores() {
        Map<Integer, ObjectStore> results = new HashMap<>();
        Configuration config = app.getConfiguration();
        Iterator<String> configIterator = config.getKeys();
        while (configIterator.hasNext()) {
            String key = configIterator.next();
            if (!key.startsWith(BounceBlobStore.STORE_PROPERTY)) {
                continue;
            }

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

    private int getStoreId(String key) {
        int indexEnd = key.indexOf(".", BounceBlobStore.STORE_PROPERTY.length() + 1);
        return Integer.decode(key.substring(BounceBlobStore.STORE_PROPERTY.length() + 1, indexEnd));
    }

    private String getFieldName(String key) {
        int indexEnd;
        if (key.indexOf(PROPERTIES_PREFIX) == -1) {
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
        Iterator<String> keyIterator = config.getKeys();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            if (!key.startsWith(BounceBlobStore.STORE_PROPERTY)) {
                continue;
            }
            // Skip the "." and the first digit
            int indexStart = key.indexOf(BounceBlobStore.STORE_PROPERTY) + BounceBlobStore.STORE_PROPERTY.length() + 1;
            int indexEnd = key.indexOf(".", indexStart);
            String indexString = key.substring(indexStart, indexEnd);
            int index = Integer.decode(indexString);
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

        public String toString() {
            return "Id: " + id + " Provider: " + provider + " Identity: " + identity + " Credential: " + credential +
                    " Nickname: " + nickname;
        }
    }
}
