/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Throwables.propagate;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import javax.annotation.Resource;
import javax.inject.Inject;

import com.bouncestorage.bounce.admin.FsckTask;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.io.ByteSource;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.ContainerAccess;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.internal.PageSetImpl;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.domain.Location;
import org.jclouds.logging.Logger;
import org.jclouds.providers.ProviderMetadata;

public final class BounceBlobStore implements BlobStore {

    public static final String STORE_PROPERTY_1 = "bounce.store.properties.1";
    public static final String STORE_PROPERTY_2 = "bounce.store.properties.2";

    public static final String LOG_MARKER_SUFFIX = "%01log";

    public enum Region {
        NEAR,
        FAR,
    }

    public static final ImmutableSet<Region> NEAR_ONLY = ImmutableSet.of(Region.NEAR);
    public static final ImmutableSet<Region> FAR_ONLY = ImmutableSet.of(Region.FAR);
    public static final ImmutableSet<Region> EVERYWHERE = ImmutableSet.of(Region.NEAR, Region.FAR);

    @Resource
    private Logger logger = Logger.NULL;

    private BlobStoreContext context;
    private BlobStore nearStore;
    private BlobStore farStore;

    @Inject
    BounceBlobStore(BlobStoreContext context, ProviderMetadata providerMetadata) {
        this.context = requireNonNull(context);
        Properties properties = providerMetadata.getDefaultProperties();

        initStores(Utils.extractProperties(properties, STORE_PROPERTY_1 + "."),
                Utils.extractProperties(properties, STORE_PROPERTY_2 + "."));
    }

    private void initStores(Properties prop1, Properties prop2) {
        this.nearStore = Utils.storeFromProperties(requireNonNull(prop1));
        this.farStore = Utils.storeFromProperties(requireNonNull(prop2));
    }

    BlobStore getNearStore() {
        return nearStore;
    }

    BlobStore getFarStore() {
        return farStore;
    }

    @Override
    public BlobStoreContext getContext() {
        return context;
    }

    @Override
    public BlobBuilder blobBuilder(String s) {
        return nearStore.blobBuilder(s);
    }

    @Override
    public Set<? extends Location> listAssignableLocations() {
        return farStore.listAssignableLocations();
    }

    @Override
    public PageSet<? extends StorageMetadata> list() {
        return nearStore.list();
    }

    @Override
    public boolean containerExists(String s) {
        return nearStore.containerExists(s);
    }

    @Override
    public boolean createContainerInLocation(Location location, String s) {
        return createContainerInLocation(location, s, CreateContainerOptions.NONE);
    }

    @Override
    public boolean createContainerInLocation(Location location, String s, CreateContainerOptions createContainerOptions) {
        return farStore.createContainerInLocation(location, s, createContainerOptions) |
                nearStore.createContainerInLocation(location, s);
    }

    @Override
    public ContainerAccess getContainerAccess(String s) {
        return nearStore.getContainerAccess(s);
    }

    @Override
    public void setContainerAccess(String s, ContainerAccess containerAccess) {
        nearStore.setContainerAccess(s, containerAccess);
        farStore.setContainerAccess(s, containerAccess);
    }

    @Override
    public PageSet<? extends StorageMetadata> list(String s) {
        return list(s, new ListContainerOptions().maxResults(1000));
    }

    private static boolean isMarkerBlob(String name) {
        return name.endsWith(LOG_MARKER_SUFFIX);
    }

    private static String markerBlobGetName(String marker) {
        return marker.substring(0, marker.length() - LOG_MARKER_SUFFIX.length());
    }

    @Override
    public PageSet<? extends StorageMetadata> list(String s, ListContainerOptions listContainerOptions) {
        PeekingIterator<StorageMetadata> nearPage = Iterators.peekingIterator(
                Utils.crawlBlobStore(nearStore, s, listContainerOptions).iterator());
        PeekingIterator<StorageMetadata> farPage = Iterators.peekingIterator(
                Utils.crawlBlobStore(farStore, s, listContainerOptions).iterator());
        TreeMap<String, BounceStorageMetadata> contents = new TreeMap<>();
        int maxResults = listContainerOptions.getMaxResults() == null ?
                1000 : listContainerOptions.getMaxResults();

        while (nearPage.hasNext() && contents.size() < maxResults) {
            StorageMetadata nearMeta = nearPage.next();
            String name = nearMeta.getName();

            logger.debug("found near blob: %s", name);
            if (isMarkerBlob(name)) {
                BounceStorageMetadata meta = contents.get(markerBlobGetName(name));
                if (meta != null) {
                    meta.hasMarkerBlob(true);
                }
                logger.debug("skipping marker blob: %s", name);
                continue;
            }

            int compare = -1;
            StorageMetadata farMeta = null;
            while (farPage.hasNext()) {
                farMeta = farPage.peek();
                compare = name.compareTo(farMeta.getName());
                if (compare <= 0) {
                    break;
                } else {
                    farPage.next();
                    logger.debug("skipping far blob: %s", farMeta.getName());
                }
            }

            if (compare == 0) {
                farPage.next();
                logger.debug("found far blob with the same name: %s", name);
                boolean nextIsMarker = false;
                if (nearPage.hasNext()) {
                    StorageMetadata next = nearPage.peek();
                    logger.debug("next blob: %s", next.getName());
                    if (next.getName().equals(name + LOG_MARKER_SUFFIX)) {
                        nextIsMarker = true;
                    }
                }

                BounceStorageMetadata meta;

                if (nextIsMarker) {
                    if (isLink(s, name)) {
                        meta = new BounceStorageMetadata(farMeta, FAR_ONLY);
                    } else if (Utils.eTagsEqual(nearMeta.getETag(), farMeta.getETag())) {
                        meta = new BounceStorageMetadata(nearMeta, EVERYWHERE);
                    } else {
                        meta = new BounceStorageMetadata(nearMeta, NEAR_ONLY);
                    }

                    meta.hasMarkerBlob(true);
                    contents.put(name, meta);
                } else {
                    if (Utils.eTagsEqual(nearMeta.getETag(), farMeta.getETag())) {
                        meta = new BounceStorageMetadata(nearMeta, EVERYWHERE);
                    } else {
                        meta = new BounceStorageMetadata(farMeta, FAR_ONLY);
                    }
                }

                contents.put(name, meta);
            } else {
                contents.put(name, new BounceStorageMetadata(nearMeta, NEAR_ONLY));
            }
        }

        if (nearPage.hasNext()) {
            StorageMetadata nearMeta = nearPage.next();
            String name = nearMeta.getName();

            logger.debug("found near blob: %s", name);
            if (isMarkerBlob(name)) {
                BounceStorageMetadata meta = contents.get(markerBlobGetName(name));
                if (meta != null) {
                    meta.hasMarkerBlob(true);
                }
            }
        }

        return new PageSetImpl<>(contents.values(),
                nearPage.hasNext() ? nearPage.next().getName() : null);
    }

    private FsckTask.Result maybeRemoveStaleFarBlob(String container, String key) {
        BlobMetadata near = nearStore.blobMetadata(container, key);
        BlobMetadata far = farStore.blobMetadata(container, key);

        if ((near == null || !BounceLink.isLink(near)) &&
                (far != null && (near == null || !Utils.eTagsEqual(near.getETag(), far.getETag())))) {
            farStore.removeBlob(container, key);
            return FsckTask.Result.REMOVED;
        }

        return FsckTask.Result.NO_OP;
    }

    // TODO: this assumes during reconciliation the blob store is not modified
    public FsckTask.Result reconcileObject(String container, BounceStorageMetadata user, StorageMetadata far) {
        String s = user != null ? user.getName() : far.getName();
        if (user != null) {
            logger.debug("reconciling %s regions=%s %s %s", s, user.getRegions(), user, far);
            if (user.getRegions() == NEAR_ONLY && far != null) {
                return maybeRemoveStaleFarBlob(container, s);
            }
        } else {
            // we have a blob that's only in far store but nothing (not even link) in near store
            return maybeRemoveStaleFarBlob(container, s);
        }

        return FsckTask.Result.NO_OP;
    }

    @Override
    public void clearContainer(String s) {
        nearStore.clearContainer(s);
        farStore.clearContainer(s);
    }

    @Override
    public void clearContainer(String s, ListContainerOptions listContainerOptions) {
        nearStore.clearContainer(s, listContainerOptions);
        farStore.clearContainer(s, listContainerOptions);
    }

    @Override
    public void deleteContainer(String s) {
        nearStore.deleteContainer(s);
        farStore.deleteContainer(s);
    }

    @Override
    public boolean deleteContainerIfEmpty(String s) {
        boolean deleted = nearStore.deleteContainerIfEmpty(s);
        if (deleted) {
            farStore.deleteContainer(s);
        }
        return deleted;
    }

    @Override
    public boolean directoryExists(String s, String s1) {
        return nearStore.directoryExists(s, s1);
    }

    @Override
    public void createDirectory(String s, String s1) {
        nearStore.createDirectory(s, s1);
    }

    @Override
    public void deleteDirectory(String s, String s1) {
        nearStore.deleteDirectory(s, s1);
    }

    @Override
    public boolean blobExists(String s, String s1) {
        return nearStore.blobExists(s, s1);
    }

    @Override
    public String putBlob(String containerName, Blob blob) {
        return putBlob(containerName, blob, PutOptions.NONE);
    }

    private void putMarkerBlob(String containerName, String key) {
        nearStore.putBlob(containerName,
                blobBuilder(key + LOG_MARKER_SUFFIX).payload(ByteSource.empty()).build());
    }

    @Override
    public String putBlob(String containerName, Blob blob, PutOptions putOptions) {
        putMarkerBlob(containerName, blob.getMetadata().getName());
        return nearStore.putBlob(containerName, blob, putOptions);
    }

    public BlobMetadata blobMetadataNoFollow(String container, String s) {
        return nearStore.blobMetadata(container, s);
    }

    @Override
    public BlobMetadata blobMetadata(String s, String s1) {
        BlobMetadata meta = nearStore.blobMetadata(s, s1);
        if (meta != null && BounceLink.isLink(meta)) {
            try {
                return BounceLink.fromBlob(nearStore.getBlob(s, s1)).getBlobMetadata();
            } catch (IOException e) {
                logger.error(e, "An error occurred while loading the metadata for blob %s in container %s",
                        s, s1);
                throw propagate(e);
            }
        } else {
            return meta;
        }
    }

    @Override
    public Blob getBlob(String containerName, String blobName) {
        return getBlob(containerName, blobName, GetOptions.NONE);
    }

    @Override
    public Blob getBlob(String containerName, String blobName, GetOptions getOptions) {
        BlobMetadata meta = nearStore.blobMetadata(containerName, blobName);
        if (BounceLink.isLink(meta)) {
            // Unbounce the object
            Blob blob = farStore.getBlob(containerName, blobName, getOptions);
            if (blob == null) {
                return blob;
            }
            nearStore.putBlob(containerName, blob);
        }
        return nearStore.getBlob(containerName, blobName, getOptions);
    }

    @Override
    public void removeBlob(String s, String s1) {
        nearStore.removeBlob(s, s1);
    }

    @Override
    public void removeBlobs(String s, Iterable<String> iterable) {
        nearStore.removeBlobs(s, iterable);
    }

    @Override
    public BlobAccess getBlobAccess(String container, String name) {
        return nearStore.getBlobAccess(container, name);
    }

    @Override
    public void setBlobAccess(String container, String name, BlobAccess access) {
        nearStore.setBlobAccess(container, name, access);
        farStore.setBlobAccess(container, name, access);
    }

    @Override
    public long countBlobs(String s) {
        return nearStore.countBlobs(s);
    }

    @Override
    public long countBlobs(String s, ListContainerOptions listContainerOptions) {
        return nearStore.countBlobs(s, listContainerOptions);
    }

    public boolean isLink(String containerName, String blobName) {
        return BounceLink.isLink(nearStore.blobMetadata(containerName, blobName));
    }

    public Blob copyBlobAndCreateBounceLink(String containerName, String blobName)
            throws IOException {
        Blob blobFrom = copyBlob(containerName, blobName);
        if (blobFrom != null) {
            createBounceLink(blobFrom.getMetadata());
        }
        return blobFrom;
    }

    public void createBounceLink(BlobMetadata blobMetadata) throws IOException {
        logger.debug("link %s", blobMetadata.getName());
        BounceLink link = new BounceLink(Optional.of(blobMetadata));
        nearStore.putBlob(blobMetadata.getContainer(), link.toBlob(nearStore));
        nearStore.removeBlob(blobMetadata.getContainer(), blobMetadata.getName() + LOG_MARKER_SUFFIX);
    }

    public Blob copyBlob(String containerName, String blobName) throws IOException {
        logger.debug("copying blob %s", blobName);
        return Utils.copyBlob(nearStore, farStore, containerName, containerName, blobName);
    }

    public void updateBlobMetadata(String containerName, String blobName, Map<String, String> userMetadata) {
        Blob blob = getBlob(containerName, blobName);
        Map<String, String> allMetadata = new HashMap<>(blob.getMetadata().getUserMetadata());
        allMetadata.putAll(userMetadata);
        blob.getMetadata().setUserMetadata(allMetadata);
        nearStore.putBlob(containerName, blob);
    }

    public void takeOver(String containerName) throws IOException {
        // TODO: hook into move service to enable parallelism and cancellation
        for (StorageMetadata sm : Utils.crawlBlobStore(farStore,
                containerName)) {
            BlobMetadata metadata = farStore.blobMetadata(containerName,
                    sm.getName());
            BounceLink link = new BounceLink(Optional.of(metadata));
            nearStore.putBlob(containerName, link.toBlob(nearStore));
        }
    }

    /**
     * Sanity check that near store and far store are in sync, if they aren't,
     * we need to perform takeover.

     * @return true if the near store and farstore are in sync
     */
    public boolean sanityCheck(String containerName) throws IOException {
        PageSet<? extends StorageMetadata> res = farStore.list(containerName);
        for (StorageMetadata sm : res) {
            BlobMetadata meta = blobMetadata(containerName, sm.getName());
            if (!Utils.equals(sm, meta)) {
                return false;
            }
        }

        return true;
    }

    @VisibleForTesting
    public Blob getFromFarStore(String containerName, String blobName) {
        return farStore.getBlob(containerName, blobName);
    }

    @VisibleForTesting
    public Blob getFromNearStore(String containerName, String blobName) {
        return nearStore.getBlob(containerName, blobName);
    }

}
