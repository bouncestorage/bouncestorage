/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Throwables.propagate;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.core.Response;

import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.Utils;
import com.bouncestorage.bounce.admin.BounceApplication;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.io.ByteSource;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.input.TeeInputStream;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.internal.BlobImpl;
import org.jclouds.blobstore.domain.internal.PageSetImpl;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.io.MutableContentMetadata;
import org.jclouds.io.Payload;

@AutoService(BouncePolicy.class)
public class WriteBackPolicy extends BouncePolicy {
    public static final String COPY_DELAY = "copyDelay";
    public static final String EVICT_DELAY = "evictDelay";
    public static final String LOG_MARKER_SUFFIX = "     bounce!log";
    protected Duration copyDelay;
    protected Duration evictDelay;
    private boolean copy;
    private boolean immediateCopy;
    private boolean evict;

    public static boolean isMarkerBlob(String name) {
        return name.endsWith(LOG_MARKER_SUFFIX);
    }

    public static String markerBlobGetName(String marker) {
        return marker.substring(0, marker.length() - LOG_MARKER_SUFFIX.length());
    }

    private static String blobGetMarkerName(String blob) {
        return blob + LOG_MARKER_SUFFIX;
    }

    private void putMarkerBlob(String containerName, String key) {
        getSource().putBlob(containerName,
                getSource().blobBuilder(blobGetMarkerName(key))
                        .payload(ByteSource.empty())
                        .contentLength(0)
                        .build());
    }

    @Override
    public String putBlob(String containerName, Blob blob, PutOptions options) {
        putMarkerBlob(containerName, blob.getMetadata().getName());
        String etag = getSource().putBlob(containerName, blob, options);
        String blobName = blob.getMetadata().getName();
        enqueueReconcile(containerName, blobName, copyDelay.getSeconds());
        return etag;
    }

    @Override
    public void removeBlob(String container, String name) {
        super.removeBlob(container, name);
        enqueueReconcile(container, name, 0);
    }

    private void enqueueReconcile(String containerName, String blobName, long delaySecond) {
        if (app != null) {
            app.executeBackgroundReconcileTask(() -> reconcileObject(containerName, blobName),
                    delaySecond, TimeUnit.SECONDS);
        }
    }

    public void init(BounceApplication app, Configuration config) {
        super.init(app, config);
        this.copyDelay = requireNonNull(Duration.parse(config.getString(COPY_DELAY)));
        this.copy = !copyDelay.isNegative();
        this.immediateCopy = copyDelay.isZero();
        this.evictDelay = requireNonNull(Duration.parse(config.getString(EVICT_DELAY)));
        this.evict = !evictDelay.isNegative();
    }

    @Override
    public String copyBlob(String fromContainer, String fromName, String toContainer, String toName, CopyOptions options) {
        if (!fromContainer.equals(toContainer)) {
            // TODO we don't support cross container copy for now, since that may involve
            // different policies
            throw new UnsupportedOperationException("cross container copy");
        }

        BlobMetadata sourceMeta = getSource().blobMetadata(fromContainer, fromName);
        if (sourceMeta == null) {
            // nothing to copy
            return null;
        }

        String etag;
        if (BounceLink.isLink(sourceMeta)) {
            // we know that the far store has a valid object
            etag = getDestination().copyBlob(fromContainer, fromName, toContainer, toName, options);
            if (etag != null) {
                Utils.createBounceLink(getSource(), getDestination().blobMetadata(toContainer, toName));
            }
        } else {
            putMarkerBlob(toContainer, toName);
            etag = getSource().copyBlob(fromContainer, fromName, toContainer, toName, options);
            if (!etag.equals(sourceMeta.getETag())) {
                // another process just updated the source blob, we could have copied a link
                // ideally copyBlob would return the copied metadata so that we don't have
                // to do another call
                sourceMeta = getSource().blobMetadata(toContainer, toName);
                if (sourceMeta != null && BounceLink.isLink(sourceMeta) && etag.equals(sourceMeta.getETag())) {
                    getSource().removeBlob(toContainer, toName);
                    throw new ClientErrorException(Response.Status.CONFLICT);
                } else {
                    // we copied an old object, this is not ideal but ok for now
                }
            }

            enqueueReconcile(toContainer, toName, copyDelay.getSeconds());
        }

        return etag;
    }

    private BounceResult reconcileObject(String container, String blob)
            throws InterruptedException, ExecutionException {
        logger.debug("reconciling {}", blob);
        BlobMetadata sourceMeta = getSource().blobMetadata(container, blob);
        BlobMetadata sourceMarkerMeta = getSource().blobMetadata(container, blobGetMarkerName(blob));
        BlobMetadata destMeta = getDestination().blobMetadata(container, blob);

        BounceStorageMetadata meta;
        if (sourceMeta != null) {
            if (destMeta != null) {
                if (sourceMarkerMeta != null) {
                    if (BounceLink.isLink(sourceMeta)) {
                        meta = new BounceStorageMetadata(destMeta, BounceStorageMetadata.FAR_ONLY);
                    } else if (Utils.eTagsEqual(sourceMeta.getETag(), destMeta.getETag())) {
                        meta = new BounceStorageMetadata(sourceMeta, BounceStorageMetadata.EVERYWHERE);
                    } else {
                        meta = new BounceStorageMetadata(sourceMeta, BounceStorageMetadata.NEAR_ONLY);
                    }
                } else {
                    if (Utils.eTagsEqual(sourceMeta.getETag(), destMeta.getETag())) {
                        meta = new BounceStorageMetadata(sourceMeta, BounceStorageMetadata.EVERYWHERE);
                    } else {
                        meta = new BounceStorageMetadata(destMeta, BounceStorageMetadata.FAR_ONLY);
                    }
                }
            } else {
                meta = new BounceStorageMetadata(sourceMeta, BounceStorageMetadata.NEAR_ONLY);
            }

            return reconcileObject(container, meta, destMeta);
        } else {
            if (sourceMarkerMeta != null) {
                getSource().removeBlob(container, blobGetMarkerName(blob));
            }
            return reconcileObject(container, null, destMeta);
        }
    }

    @Override
    public BounceResult reconcileObject(String container, BounceStorageMetadata sourceObject, StorageMetadata
            destinationObject) {
        if (sourceObject != null) {
            try {
                if (evict && isObjectExpired(sourceObject, evictDelay)) {
                    return maybeMoveObject(container, sourceObject, destinationObject);
                } else if (copy && (immediateCopy || isObjectExpired(sourceObject, copyDelay))) {
                    return maybeCopyObject(container, sourceObject, destinationObject);
                }
            } catch (IOException e) {
                throw propagate(e);
            }

            return BounceResult.NO_OP;
        }

        return maybeRemoveDestinationObject(container, destinationObject);
    }

    protected boolean isObjectExpired(StorageMetadata metadata, Duration duration) {
        Instant now = app.getClock().instant();
        Instant then = metadata.getLastModified().toInstant();
        return !now.minus(duration).isBefore(then);
    }

    private Blob pipeBlobAndReturn(String container, Blob blob) throws IOException {
        PipedInputStream pipeIn = new PipedInputStream();
        PipedOutputStream pipeOut = new PipedOutputStream(pipeIn);

        Payload blobPayload = blob.getPayload();
        MutableContentMetadata contentMetadata = blob.getMetadata().getContentMetadata();
        Blob retBlob = new BlobImpl(blob.getMetadata());
        retBlob.setPayload(pipeIn);
        retBlob.setAllHeaders(blob.getAllHeaders());
        TeeInputStream tee = new TeeInputStream(blobPayload.openStream(), pipeOut, true);
        retBlob.getMetadata().setContentMetadata(contentMetadata);

        app.executeBackgroundTask(() -> {
            try {
                return Utils.copyBlob(getSource(), container, blob, tee);
            } finally {
                tee.close();
            }
        });
        return retBlob;
    }

    @Override
    public Blob getBlob(String container, String blobName, GetOptions options) {
        Blob blob = super.getBlob(container, blobName, options);
        if (blob == null) {
            if (takeOverInProcess) {
                return getDestination().getBlob(container, blobName, options);
            }
            return null;
        }
        BlobMetadata meta = blob.getMetadata();
        if (BounceLink.isLink(meta)) {
            try {
                if (app != null && options.equals(GetOptions.NONE)) {
                    blob = getDestination().getBlob(container, blobName, GetOptions.NONE);
                    return pipeBlobAndReturn(container, blob);
                } else {
                    // fallback to the dumb thing and do double streaming
                    Utils.copyBlob(getDestination(), getSource(), container, container, blobName);
                    return getSource().getBlob(container, blobName, options);
                }
            } catch (IOException e) {
                e.printStackTrace();
                return getDestination().getBlob(container, blobName, options);
            }
        } else {
            return blob;
        }
    }

    @Override
    public BlobMetadata blobMetadata(String container, String blobName) {
        BlobMetadata meta = getSource().blobMetadata(container, blobName);
        if (meta != null) {
            if (BounceLink.isLink(meta)) {
                Blob linkBlob = getSource().getBlob(container, blobName);
                if (linkBlob != null) {
                    try {
                        return BounceLink.fromBlob(linkBlob).getBlobMetadata();
                    } catch (IOException e) {
                        throw propagate(e);
                    }
                } else {
                    return null;
                }
            }
        }
        return meta;
    }

    protected final BounceResult maybeMoveObject(String container, BounceStorageMetadata sourceObject,
            StorageMetadata destinationObject) throws IOException {
        if (sourceObject.getRegions().equals(BounceStorageMetadata.FAR_ONLY)) {
            return BounceResult.NO_OP;
        }
        if (sourceObject.getRegions().equals(BounceStorageMetadata.EVERYWHERE)) {
            BlobMetadata sourceMetadata = getSource().blobMetadata(container, sourceObject.getName());
            BlobMetadata destinationMetadata = getDestination().blobMetadata(container, destinationObject.getName());

            if (sourceMetadata != null && destinationMetadata != null &&
                    Utils.eTagsEqual(destinationMetadata.getETag(), sourceMetadata.getETag())) {
                Utils.createBounceLink(this, sourceMetadata);
                return BounceResult.LINK;
            }
        }

        Utils.copyBlobAndCreateBounceLink(getSource(), getDestination(), container,
                sourceObject.getName());
        return BounceResult.MOVE;
    }

    @Override
    public final PageSet<? extends StorageMetadata> list(String s, ListContainerOptions listContainerOptions) {
        if (takeOverInProcess) {
            return getDestination().list(s, listContainerOptions);
        }
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
            if (WriteBackPolicy.isMarkerBlob(name)) {
                BounceStorageMetadata meta = contents.get(WriteBackPolicy.markerBlobGetName(name));
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
                    if (next.getName().equals(blobGetMarkerName(name))) {
                        nextIsMarker = true;
                    }
                }

                BounceStorageMetadata meta;
                ImmutableSet<BounceStorageMetadata.Region> farRegions = translateRegions(farMeta);

                if (nextIsMarker) {
                    if (BounceLink.isLink(getSource().blobMetadata(s, name))) {
                        meta = new BounceStorageMetadata(farMeta, farRegions);
                    } else if (Utils.eTagsEqual(nearMeta.getETag(), farMeta.getETag())) {
                        meta = new BounceStorageMetadata(nearMeta,
                                new ImmutableSet.Builder<BounceStorageMetadata.Region>()
                                        .addAll(farRegions)
                                        .add(BounceStorageMetadata.Region.NEAR)
                                        .build());
                    } else {
                        meta = new BounceStorageMetadata(nearMeta, BounceStorageMetadata.NEAR_ONLY);
                    }

                    meta.hasMarkerBlob(true);
                    contents.put(name, meta);
                } else {
                    if (Utils.eTagsEqual(nearMeta.getETag(), farMeta.getETag())) {
                        meta = new BounceStorageMetadata(nearMeta,
                                new ImmutableSet.Builder<BounceStorageMetadata.Region>()
                                        .add(BounceStorageMetadata.Region.NEAR)
                                        .addAll(farRegions)
                                        .build());
                    } else {
                        meta = new BounceStorageMetadata(farMeta, farRegions);
                    }
                }

                contents.put(name, meta);
            } else {
                contents.put(name, new BounceStorageMetadata(nearMeta, BounceStorageMetadata.NEAR_ONLY));
            }
        }

        if (nearPage.hasNext()) {
            StorageMetadata nearMeta = nearPage.next();
            String name = nearMeta.getName();

            logger.debug("found near blob: {}", name);
            if (WriteBackPolicy.isMarkerBlob(name)) {
                BounceStorageMetadata meta = contents.get(WriteBackPolicy.markerBlobGetName(name));
                if (meta != null) {
                    meta.hasMarkerBlob(true);
                }
            }
        }

        return new PageSetImpl<>(contents.values(),
                nearPage.hasNext() ? nearPage.next().getName() : null);
    }

    private ImmutableSet<BounceStorageMetadata.Region> translateRegions(StorageMetadata farMetadata) {
        if (!(farMetadata instanceof BounceStorageMetadata)) {
            return BounceStorageMetadata.FAR_ONLY;
        }
        ImmutableSet<BounceStorageMetadata.Region> regions = ((BounceStorageMetadata) farMetadata).getRegions();
        if (regions.equals(BounceStorageMetadata.FAR_ONLY)) {
            return ImmutableSet.of(BounceStorageMetadata.Region.FARTHER);
        } else if (regions.equals(BounceStorageMetadata.NEAR_ONLY)) {
            return ImmutableSet.of(BounceStorageMetadata.Region.FAR);
        } else {
            return ImmutableSet.of(BounceStorageMetadata.Region.FAR, BounceStorageMetadata.Region.FARTHER);
        }
    }

    protected final BounceResult maybeRemoveDestinationObject(String container, StorageMetadata object) {
        requireNonNull(object);

        BlobMetadata sourceMeta = getSource().blobMetadata(container, object.getName());
        BlobMetadata destinationMeta = getDestination().blobMetadata(container, object.getName());
        if (sourceMeta == null && destinationMeta != null) {
            getDestination().removeBlob(container, object.getName());
            return BounceResult.REMOVE;
        }

        if (sourceMeta != null && destinationMeta != null && !Utils.eTagsEqual(sourceMeta.getETag(),
                destinationMeta.getETag())) {
            getDestination().removeBlob(container, destinationMeta.getName());
            return BounceResult.REMOVE;
        }

        return BounceResult.NO_OP;
    }

    protected final BounceResult maybeCopyObject(String container, BounceStorageMetadata sourceObject,
            StorageMetadata destinationObject) throws IOException {
        if (sourceObject.getRegions().equals(BounceStorageMetadata.FAR_ONLY)) {
            return BounceResult.NO_OP;
        }
        if (sourceObject.getRegions().equals(BounceStorageMetadata.EVERYWHERE)) {
            BlobMetadata destinationMeta = getDestination().blobMetadata(container, destinationObject.getName());
            BlobMetadata sourceMeta = getSource().blobMetadata(container, sourceObject.getName());
            if (Utils.eTagsEqual(destinationMeta.getETag(), sourceMeta.getETag())) {
                return BounceResult.NO_OP;
            }
        }

        // Either the object does not exist in the far store or the ETags are not equal, so we should copy
        Utils.copyBlob(getSource(), getDestination(), container, container, sourceObject.getName());
        return BounceResult.COPY;
    }
}
