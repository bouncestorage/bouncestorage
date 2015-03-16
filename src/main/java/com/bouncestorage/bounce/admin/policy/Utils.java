/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin.policy;

import static com.google.common.base.Throwables.propagate;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Optional;

import com.bouncestorage.bounce.BounceLink;
import com.bouncestorage.bounce.admin.BouncePolicy;
import com.google.common.hash.HashCode;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.io.ContentMetadata;

public final class Utils {
    private Utils() {
        throw new AssertionError("Intentionally not implemented");
    }

    static BouncePolicy.BounceResult maybeRemoveObject(BlobStore store, String container, StorageMetadata
            objectMetadata) {
        if (objectMetadata == null) {
            return BouncePolicy.BounceResult.NO_OP;
        }

        store.removeBlob(container, objectMetadata.getName());
        return BouncePolicy.BounceResult.REMOVE;
    }

    // TODO: eventually this should support parallel copies, cancellation, and
    // multi-part uploads
    public static Blob copyBlob(BlobStore from, BlobStore to,
                                String containerNameFrom, String containerNameTo, String blobName)
            throws IOException {
        Blob blobFrom = from.getBlob(containerNameFrom, blobName);
        if (blobFrom == null || BounceLink.isLink(blobFrom.getMetadata())) {
            return null;
        }
        ContentMetadata metadata = blobFrom.getMetadata().getContentMetadata();
        try (InputStream is = blobFrom.getPayload().openStream()) {
            BlobBuilder.PayloadBlobBuilder builder = to.blobBuilder(blobName)
                    .userMetadata(blobFrom.getMetadata().getUserMetadata())
                    .payload(is);

            String contentDisposition = metadata.getContentDisposition();
            if (contentDisposition != null) {
                builder.contentDisposition(contentDisposition);
            }

            String contentEncoding = metadata.getContentEncoding();
            if (contentEncoding != null) {
                builder.contentEncoding(contentEncoding);
            }

            String contentLanguage = metadata.getContentLanguage();
            if (contentLanguage != null) {
                builder.contentLanguage(contentLanguage);
            }

            HashCode contentMd5 = metadata.getContentMD5AsHashCode();
            if (contentMd5 != null) {
                builder.contentMD5(contentMd5);
            }

            Long contentLength = metadata.getContentLength();
            if (contentLength != null) {
                builder.contentLength(metadata.getContentLength());
            }

            String contentType = metadata.getContentType();
            if (contentType != null) {
                builder.contentType(metadata.getContentType());
            }

            Date expires = metadata.getExpires();
            if (expires != null) {
                builder.expires(expires);
            }

            to.putBlob(containerNameTo, builder.build());
            return blobFrom;
        }
    }

    static BouncePolicy.BounceResult copyBlobAndCreateBounceLink(BouncePolicy policy, String containerName, String
            blobName) {
        try {
            Blob blobFrom = Utils.copyBlob(policy.getSource(), policy.getDestination(), containerName, containerName,
                    blobName);
            if (blobFrom != null) {
                createBounceLink(policy, blobFrom.getMetadata());
            }
        } catch (IOException e) {
            propagate(e);
        }
        return BouncePolicy.BounceResult.MOVE;
    }

    static BouncePolicy.BounceResult createBounceLink(BouncePolicy policy, BlobMetadata blobMetadata) {
        policy.getLogger().info("link %s", blobMetadata.getName());
        BounceLink link = new BounceLink(Optional.of(blobMetadata));
        policy.getSource().putBlob(blobMetadata.getContainer(), link.toBlob(policy.getSource()));
        policy.getSource().removeBlob(blobMetadata.getContainer(), blobMetadata.getName() +
                MarkerPolicy.LOG_MARKER_SUFFIX);
        return BouncePolicy.BounceResult.LINK;
    }
}
