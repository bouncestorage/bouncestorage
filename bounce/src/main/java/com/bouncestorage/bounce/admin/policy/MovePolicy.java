/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static com.google.common.base.Throwables.propagate;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import com.bouncestorage.bounce.BounceBlobStore;
import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.BounceStorageMetadata;
import com.bouncestorage.bounce.Utils;

import org.apache.commons.io.input.TeeInputStream;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.io.MutableContentMetadata;

public abstract class MovePolicy extends MarkerPolicy {

    private Blob pipeBlobAndReturn(String container, Blob blob) throws IOException {
        PipedInputStream pipeIn = new PipedInputStream();
        PipedOutputStream pipeOut = new PipedOutputStream(pipeIn);

        TeeInputStream tee = new TeeInputStream(blob.getPayload().openStream(), pipeOut, true);
        MutableContentMetadata contentMetadata = blob.getMetadata().getContentMetadata();
        blob.setPayload(pipeIn);
        blob.getMetadata().setContentMetadata(contentMetadata);

        app.executeBackgroundTask(() -> {
            try {
                Utils.copyBlob(getSource(), container, blob, tee);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return blob;
    }

    @Override
    public Blob getBlob(String container, String blobName, GetOptions options) {
        Blob blob = super.getBlob(container, blobName, options);
        if (blob == null) {
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
        if (sourceObject.getRegions().equals(BounceBlobStore.FAR_ONLY)) {
            return BounceResult.NO_OP;
        }
        if (sourceObject.getRegions().equals(BounceBlobStore.EVERYWHERE)) {
            BlobMetadata sourceMetadata = getSource().blobMetadata(container, sourceObject.getName());
            BlobMetadata destinationMetadata = getDestination().blobMetadata(container, destinationObject.getName());
            if (Utils.eTagsEqual(destinationMetadata.getETag(), sourceMetadata.getETag())) {
                Utils.createBounceLink(this, sourceMetadata);
                return BounceResult.LINK;
            }
        }

        Utils.copyBlobAndCreateBounceLink(getSource(), getDestination(), container,
                sourceObject.getName());
        return BounceResult.MOVE;
    }
}
