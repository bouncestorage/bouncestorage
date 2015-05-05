/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static java.util.Objects.requireNonNull;

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

import com.codahale.metrics.annotation.Timed;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;

@Path("/object_store/{id}/container")
@Produces(MediaType.APPLICATION_JSON)
public final class ContainerResource {
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
        blobStore.createContainerInLocation(null, request.get("name"));
        return Response.ok().build();
    }

    @GET
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

    private static class ContainerMapEntry {
        private int virtualContainerId;
        private Container.ContainerStatus status;
    }

    private static class Container {
        enum ContainerStatus { UNCONFIGURED, CONFIGURED, INUSE }

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
