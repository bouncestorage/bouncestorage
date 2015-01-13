/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

import com.google.common.base.Preconditions;
import com.google.common.hash.HashCode;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder.PayloadBlobBuilder;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.io.ContentMetadata;

public final class Utils {
    private Utils() {
        throw new AssertionError("intentionally unimplemented");
    }

    public static Iterable<StorageMetadata> crawlBlobStore(
            BlobStore blobStore, String containerName) {
        return crawlBlobStore(blobStore, containerName,
                new ListContainerOptions());
    }

    public static Iterable<StorageMetadata> crawlBlobStore(
            BlobStore blobStore, String containerName,
            ListContainerOptions options) {
        return new CrawlBlobStoreIterable(blobStore, containerName, options);
    }

    static Properties propertiesFromFile(File file) throws IOException {
        Properties properties = new Properties();
        try (InputStream is = new FileInputStream(file)) {
            properties.load(is);
        }
        return properties;
    }

    private static class CrawlBlobStoreIterable
            implements Iterable<StorageMetadata> {
        private final BlobStore blobStore;
        private final String containerName;
        private final ListContainerOptions options;

        CrawlBlobStoreIterable(BlobStore blobStore, String containerName,
                ListContainerOptions options) {
            this.blobStore = Preconditions.checkNotNull(blobStore);
            this.containerName = Preconditions.checkNotNull(containerName);
            this.options = Preconditions.checkNotNull(options);
        }

        @Override
        public Iterator<StorageMetadata> iterator() {
            return new CrawlBlobStoreIterator(blobStore, containerName,
                    options);
        }
    }

    private static class CrawlBlobStoreIterator
            implements Iterator<StorageMetadata> {
        private final BlobStore blobStore;
        private final String containerName;
        private final ListContainerOptions options;
        private Iterator<? extends StorageMetadata> iterator;
        private String marker;

        CrawlBlobStoreIterator(BlobStore blobStore, String containerName,
                ListContainerOptions options) {
            this.blobStore = Preconditions.checkNotNull(blobStore);
            this.containerName = Preconditions.checkNotNull(containerName);
            this.options = Preconditions.checkNotNull(options);
            advance();
        }

        private void advance() {
            if (marker != null) {
                options.afterMarker(marker);
            }
            PageSet<? extends StorageMetadata> set = blobStore.list(
                    containerName, options);
            marker = set.getNextMarker();
            iterator = set.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext() || marker != null;
        }

        @Override
        public StorageMetadata next() {
            if (!iterator.hasNext()) {
                advance();
            }
            return iterator.next();
        }
    }

    // TODO: eventually this should support parallel copies, cancellation, and
    // multi-part uploads
    static Blob copyBlob(BlobStore from, BlobStore to,
            String containerNameFrom, String containerNameTo, String blobName)
            throws IOException {
        Blob blobFrom = from.getBlob(containerNameFrom, blobName);
        if (blobFrom == null) {
            return null;
        }
        ContentMetadata metadata = blobFrom.getMetadata().getContentMetadata();
        try (InputStream is = blobFrom.getPayload().openStream()) {
            PayloadBlobBuilder builder = to.blobBuilder(blobName).payload(is);

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

    static void moveBlob(BlobStore from, BlobStore to,
            String containerNameFrom, String containerNameTo, String blobName)
            throws IOException {
        copyBlob(from, to, containerNameFrom, containerNameTo, blobName);
        from.removeBlob(containerNameFrom, blobName);
    }
}
