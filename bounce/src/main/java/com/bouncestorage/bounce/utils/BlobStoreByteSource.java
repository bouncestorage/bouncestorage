/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.utils;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Throwables.propagate;

import java.io.IOException;
import java.io.InputStream;

import com.google.common.io.ByteSource;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.io.Payload;

public final class BlobStoreByteSource extends ByteSource {
    private final BlobStore blobStore;
    private final String container;
    private final String blobName;
    private final long size;
    private GetOptions options;
    private final boolean isStreamRepeatable;
    private Payload payload;

    public BlobStoreByteSource(BlobStore blobStore, Blob blob, long size) throws IOException {
        this(blobStore, blob, GetOptions.NONE, size);
    }

    public BlobStoreByteSource(BlobStore blobStore, Blob blob, GetOptions options, long size) throws IOException {
        requireNonNull(blob);
        this.blobStore = requireNonNull(blobStore);
        this.container = blob.getMetadata().getContainer();
        this.blobName = blob.getMetadata().getName();
        this.size = size;
        this.options = requireNonNull(options);
        this.payload = blob.getPayload();
        this.isStreamRepeatable = blob.getPayload().isRepeatable();
    }

    @Override
    public long size() throws IOException {
        return size;
    }

    @Override
    public ByteSource slice(long offset, long length) {
        long remaining = Math.min(size - offset, length);
        GetOptions getOptions = new GetOptions().range(offset, offset + remaining - 1);

        try {
            return new BlobStoreByteSource(blobStore,
                    blobStore.getBlob(container, blobName, getOptions), getOptions, remaining);
        } catch (IOException e) {
            throw propagate(e);
        }
    }

    @Override
    public InputStream openStream() throws IOException {
        if (payload != null) {
            Payload in = payload;
            if (!isStreamRepeatable) {
                // once we return the stream, we assume that it will be consumed
                payload = null;
            }
            return in.openStream();
        } else {
            return blobStore.getBlob(container, blobName, options).getPayload().openStream();
        }
    }
}
