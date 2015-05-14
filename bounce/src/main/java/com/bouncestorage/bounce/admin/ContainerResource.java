/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static java.util.Objects.requireNonNull;

import static com.bouncestorage.bounce.admin.ObjectStoreResource.ObjectStore;
import static com.bouncestorage.bounce.admin.ObjectStoreResource.getStoreById;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.bouncestorage.bounce.BounceStorageMetadata;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.googlecloud.domain.ListPage;
import org.jclouds.googlecloudstorage.GoogleCloudStorageApi;
import org.jclouds.googlecloudstorage.domain.Bucket;
import org.jclouds.googlecloudstorage.domain.DomainResourceReferences;
import org.jclouds.googlecloudstorage.domain.templates.BucketTemplate;
import org.jclouds.googlecloudstorage.features.BucketApi;
import org.jclouds.rest.ResourceAlreadyExistsException;

@Path("/object_store/{id}/container")
@Produces(MediaType.APPLICATION_JSON)
public final class ContainerResource {
    private static final int MAX_OBJECTS = 50;

    private final BounceApplication app;

    public ContainerResource(BounceApplication app) {
        this.app = requireNonNull(app);
    }

    @POST
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createContainer(@PathParam("id") int providerId, Map<String, String> request) {
        BlobStore blobStore = app.getBlobStore(providerId);
        if (blobStore == null) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        if (!request.containsKey("name")) {
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }
        ObjectStore store = getStoreById(providerId, app.getConfiguration());
        if (store.getStorageClass() == null && store.getRegion() == null) {
            blobStore.createContainerInLocation(null, request.get("name"));
            return Response.ok().build();
        }

        switch (store.getProvider()) {
            case "google-cloud-storage":
                GoogleCloudStorageApi api = blobStore.getContext().unwrapApi(GoogleCloudStorageApi.class);
                BucketApi bucketApi = api.getBucketApi();
                BucketTemplate template = new BucketTemplate().name(request.get("name"))
                        .location(DomainResourceReferences.Location.valueOf(store.getRegion()))
                        .storageClass(DomainResourceReferences.StorageClass.valueOf(store.translateStorageClass()));
                Bucket bucket = bucketApi.createBucket(store.translateIdentity(), template);
                if (bucket == null) {
                    return Response.status(Response.Status.CONFLICT).build();
                }
                break;
            case "aws-s3":
                try {
                    if (!blobStore.createContainerInLocation(getLocation(blobStore, store.getRegion()), request.get("name"))) {
                        return Response.status(Response.Status.BAD_REQUEST).build();
                    }
                } catch (ResourceAlreadyExistsException e) {
                    return Response.status(Response.Status.CONFLICT).build();
                }
                break;
            default:
                return Response.status(Response.Status.BAD_REQUEST).build();
        }
        return Response.ok().build();
    }

    @GET
    @Timed
    @Path("/{name}")
    public Container getContainer(@PathParam("id") int providerId, @PathParam("name") String containerName) {
        BlobStore blobStore = app.getBlobStore(containerName);
        if (containerName == null) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        if (blobStore == null) {
            // This occurs for containers that are not virtual containers
            blobStore = app.getBlobStore(providerId);
        }
        if (blobStore == null) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        if (!blobStore.containerExists(containerName)) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        List<ContainerObject> objects = blobStore.list(containerName,
                ListContainerOptions.Builder.maxResults(MAX_OBJECTS))
                .stream()
                .map(sm -> {
                    ImmutableSet<BounceStorageMetadata.Region> regions;
                    if (sm instanceof BounceStorageMetadata) {
                        BounceStorageMetadata bounceMeta = (BounceStorageMetadata) sm;
                        regions = bounceMeta.getRegions();
                    } else {
                        regions = BounceStorageMetadata.NEAR_ONLY;
                    }
                    return new ContainerObject(sm.getName(), sm.getSize(), regions);
                })
                .collect(Collectors.toList());
        Container container = createContainerObject(containerName, createVirtualContainerMap(providerId));
        container.objects = objects;
        return container;
    }

    @GET
    @Timed
    public List<Container> getContainerList(@PathParam("id") int providerId) {
        BlobStore blobStore = app.getBlobStore(providerId);
        ObjectStore store = getStoreById(providerId, app.getConfiguration());
        if (blobStore == null) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        Map<String, ContainerMapEntry> containerMap = createVirtualContainerMap(providerId);
        if (store.getStorageClass() == null) {
            PageSet<? extends StorageMetadata> pageSet = blobStore.list();
            return pageSet.stream()
                    .filter(sm -> true)
                    .map(sm -> createContainerObject(sm.getName(), containerMap))
                    .collect(Collectors.toList());
        }

        GoogleCloudStorageApi googleApi = blobStore.getContext().unwrapApi(GoogleCloudStorageApi.class);
        ListPage<Bucket> listPage = googleApi.getBucketApi().listBucket(store.translateIdentity());
        return listPage.stream()
                .filter(bucket -> bucket.storageClass() ==
                        DomainResourceReferences.StorageClass.valueOf(store.translateStorageClass()))
                .filter(bucket -> bucket.location().name().equalsIgnoreCase(store.getRegion()))
                .map(bucket -> createContainerObject(bucket.name(), containerMap))
                .collect(Collectors.toList());
    }

    private org.jclouds.domain.Location getLocation(BlobStore blobStore, String region) {
        return blobStore.listAssignableLocations().stream().filter(location -> location.getId().equals(region))
                .findFirst().orElse(null);
    }

    private Map<String, ContainerMapEntry> createVirtualContainerMap(int providerId) {
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
        return containerMap;
    }

    private Container createContainerObject(String name, Map<String, ContainerMapEntry> containerMap) {
        Container container = new Container(name);
        if (containerMap.containsKey(container.name)) {
            ContainerMapEntry entry = containerMap.get(container.name);
            container.status = entry.status;
            container.virtualContainerId = entry.virtualContainerId;
        }
        return container;
    }

    private static class ContainerMapEntry {
        private int virtualContainerId;
        private Container.ContainerStatus status;
    }

    private static class Container {
        enum ContainerStatus { UNCONFIGURED, CONFIGURED, INUSE }

        @JsonProperty
        private String name;
        @JsonProperty
        private ContainerStatus status;
        @JsonProperty
        private int virtualContainerId;
        @JsonProperty
        private List<ContainerObject> objects;

        Container(String name) {
            this.name = name;
            virtualContainerId = -1;
            status = ContainerStatus.UNCONFIGURED;
        }
    }

    private static class ContainerObject {
        enum Tier { CACHE, PRIMARY, ARCHIVE, MIGRATED }
        @JsonProperty
        String name;
        @JsonProperty
        long size;
        @JsonProperty
        ImmutableSet<BounceStorageMetadata.Region> regions;

        public ContainerObject(String name, long size, ImmutableSet<BounceStorageMetadata.Region> regions) {
            this.name = name;
            this.size = size;
            this.regions = regions;
        }
    }
}
