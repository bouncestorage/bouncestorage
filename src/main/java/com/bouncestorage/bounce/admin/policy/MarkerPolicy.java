/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

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
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.internal.PageSetImpl;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;

public abstract class MarkerPolicy implements BouncePolicy {
    public static final String LOG_MARKER_SUFFIX = "%01log";

    public static boolean isMarkerBlob(String name) {
        return name.endsWith(LOG_MARKER_SUFFIX);
    }

    public static String markerBlobGetName(String marker) {
        return marker.substring(0, marker.length() - LOG_MARKER_SUFFIX.length());
    }

    @Override
    public final String putBlob(String container, Blob blob, PutOptions options) {
        putMarkerBlob(container, blob.getMetadata().getName());
        return getSource().putBlob(container, blob, options);
    }

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

            getLogger().info("found near blob: %s", name);
            if (MarkerPolicy.isMarkerBlob(name)) {
                BounceStorageMetadata meta = contents.get(MarkerPolicy.markerBlobGetName(name));
                if (meta != null) {
                    meta.hasMarkerBlob(true);
                }
                getLogger().info("skipping marker blob: %s", name);
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
                    getLogger().info("skipping far blob: %s", farMeta.getName());
                }
            }

            if (compare == 0) {
                farPage.next();
                getLogger().info("found far blob with the same name: %s", name);
                boolean nextIsMarker = false;
                if (nearPage.hasNext()) {
                    StorageMetadata next = nearPage.peek();
                    getLogger().info("next blob: %s", next.getName());
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

            getLogger().info("found near blob: %s", name);
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
}
