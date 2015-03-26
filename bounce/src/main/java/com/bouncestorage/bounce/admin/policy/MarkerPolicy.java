/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.TreeMap;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.io.ByteSource;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.internal.PageSetImpl;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;

public abstract class MarkerPolicy extends BouncePolicy {
    public static final String LOG_MARKER_SUFFIX = "\u0001log";

    public static boolean isMarkerBlob(String name) {
        return name.endsWith(LOG_MARKER_SUFFIX);
    }

    public static String markerBlobGetName(String marker) {
        return marker.substring(0, marker.length() - LOG_MARKER_SUFFIX.length());
    }

    @Override
    public final String putBlob(String container, Blob blob, PutOptions options) {
        putMarkerBlob(container, blob.getMetadata().getName());
        String result = getSource().putBlob(container, blob, options);
        onPut(container, blob, options);
        return result;
    }

    public abstract void onPut(String container, Blob blob, PutOptions options);

    @Override
    public final PageSet<? extends StorageMetadata> list(String s, ListContainerOptions listContainerOptions) {
        PeekingIterator<StorageMetadata> nearPage = Iterators.peekingIterator(
                Utils.crawlBlobStore(getSource(), s, listContainerOptions).iterator());
        PeekingIterator<StorageMetadata> farPage = Iterators.peekingIterator(
                Utils.crawlBlobStore(getDestination(), s, listContainerOptions).iterator());
        TreeMap<String, BounceStorageMetadata> contents = new TreeMap<>();
        int maxResults = listContainerOptions.getMaxResults() == null ?
                1000 : listContainerOptions.getMaxResults();

        while (nearPage.hasNext() && contents.size() < maxResults) {
            StorageMetadata nearMeta = nearPage.next();
            String name = nearMeta.getName();

            logger.debug("found near blob: {}", name);
            if (MarkerPolicy.isMarkerBlob(name)) {
                BounceStorageMetadata meta = contents.get(MarkerPolicy.markerBlobGetName(name));
                if (meta != null) {
                    meta.hasMarkerBlob(true);
                }
                logger.debug("skipping marker blob: {}", name);
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
                    logger.debug("skipping far blob: {}", farMeta.getName());
                }
            }

            if (compare == 0) {
                farPage.next();
                logger.debug("found far blob with the same name: {}", name);
                boolean nextIsMarker = false;
                if (nearPage.hasNext()) {
                    StorageMetadata next = nearPage.peek();
                    logger.debug("next blob: {}", next.getName());
                    if (next.getName().equals(name + MarkerPolicy.LOG_MARKER_SUFFIX)) {
                        nextIsMarker = true;
                    }
                }

                BounceStorageMetadata meta;

                if (nextIsMarker) {
                    if (BounceLink.isLink(getSource().blobMetadata(s, name))) {
                        meta = new BounceStorageMetadata(farMeta, BounceBlobStore.FAR_ONLY);
                    } else if (nearMeta.getETag().equals(farMeta.getETag())) {
                        meta = new BounceStorageMetadata(nearMeta, BounceBlobStore.EVERYWHERE);
                    } else {
                        meta = new BounceStorageMetadata(nearMeta, BounceBlobStore.NEAR_ONLY);
                    }

                    meta.hasMarkerBlob(true);
                    contents.put(name, meta);
                } else {
                    if (nearMeta.getETag().equals(farMeta.getETag())) {
                        meta = new BounceStorageMetadata(nearMeta, BounceBlobStore.EVERYWHERE);
                    } else {
                        meta = new BounceStorageMetadata(farMeta, BounceBlobStore.FAR_ONLY);
                    }
                }

                contents.put(name, meta);
            } else {
                contents.put(name, new BounceStorageMetadata(nearMeta, BounceBlobStore.NEAR_ONLY));
            }
        }

        if (nearPage.hasNext()) {
            StorageMetadata nearMeta = nearPage.next();
            String name = nearMeta.getName();

            logger.debug("found near blob: {}", name);
            if (MarkerPolicy.isMarkerBlob(name)) {
                BounceStorageMetadata meta = contents.get(MarkerPolicy.markerBlobGetName(name));
                if (meta != null) {
                    meta.hasMarkerBlob(true);
                }
            }
        }

        return new PageSetImpl<>(contents.values(),
                nearPage.hasNext() ? nearPage.next().getName() : null);
    }

    private void putMarkerBlob(String containerName, String key) {
        getSource().putBlob(containerName,
                getSource().blobBuilder(key + MarkerPolicy.LOG_MARKER_SUFFIX).payload(ByteSource.empty()).build());
    }

    protected final BounceResult maybeRemoveDestinationObject(String container, StorageMetadata object) {
        requireNonNull(object);

        BlobMetadata sourceMeta = getSource().blobMetadata(container, object.getName());
        BlobMetadata destinationMeta = getDestination().blobMetadata(container, object.getName());
        if (sourceMeta == null && destinationMeta != null) {
            getDestination().removeBlob(container, object.getName());
            return BounceResult.REMOVE;
        }

        if (sourceMeta != null && destinationMeta != null && !sourceMeta.getETag().equalsIgnoreCase(destinationMeta
                .getETag())) {
            getDestination().removeBlob(container, destinationMeta.getName());
            return BounceResult.REMOVE;
        }

        return BounceResult.NO_OP;
    }

    protected final BounceResult maybeCopyObject(String container, BounceStorageMetadata sourceObject,
            StorageMetadata destinationObject) throws IOException {
        if (sourceObject.getRegions().equals(BounceBlobStore.FAR_ONLY)) {
            return BouncePolicy.BounceResult.NO_OP;
        }
        if (sourceObject.getRegions().equals(BounceBlobStore.EVERYWHERE)) {
            BlobMetadata destinationMeta = getDestination().blobMetadata(container, destinationObject.getName());
            BlobMetadata sourceMeta = getSource().blobMetadata(container, sourceObject.getName());
            if (destinationMeta.getETag().equalsIgnoreCase(sourceMeta.getETag())) {
                return BouncePolicy.BounceResult.NO_OP;
            }
        }

        // Either the object does not exist in the far store or the ETags are not equal, so we should copy
        Utils.copyBlob(getSource(), getDestination(), container, container, sourceObject.getName());
        return BouncePolicy.BounceResult.COPY;
    }

    protected final BounceResult maybeMoveObject(String container, BounceStorageMetadata sourceObject,
            StorageMetadata destinationObject) throws IOException {
        if (sourceObject.getRegions().equals(BounceBlobStore.FAR_ONLY)) {
            return BounceResult.NO_OP;
        }
        if (sourceObject.getRegions().equals(BounceBlobStore.EVERYWHERE)) {
            BlobMetadata sourceMetadata = getSource().blobMetadata(container, sourceObject.getName());
            BlobMetadata destinationMetadata = getDestination().blobMetadata(container, destinationObject.getName());
            if (destinationMetadata.getETag().equalsIgnoreCase(sourceMetadata.getETag())) {
                Utils.createBounceLink(this, sourceMetadata);
                return BounceResult.LINK;
            }
        }

        Utils.copyBlobAndCreateBounceLink(getSource(), getDestination(), container,
                sourceObject.getName());
        return BounceResult.MOVE;
    }
}
