/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashCode;
import com.google.inject.Module;

import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder.PayloadBlobBuilder;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.io.ContentMetadata;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;

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

    public static Properties propertiesFromFile(File file) throws IOException {
        Properties properties = new Properties();
        try (InputStream is = new FileInputStream(file)) {
            properties.load(is);
        }
        return properties;
    }

    public static BlobStore storeFromProperties(Properties properties) {
        String provider = properties.getProperty(Constants.PROPERTY_PROVIDER);
        ContextBuilder builder = ContextBuilder
                .newBuilder(provider)
                .modules(ImmutableList.<Module>of(new SLF4JLoggingModule()))
                .overrides(properties);
        BlobStoreContext context = builder.build(BlobStoreContext.class);
        return context.getBlobStore();
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
            this.options = Preconditions.checkNotNull(options).recursive();
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
            while (true) {
                if (!iterator.hasNext()) {
                    advance();
                }
                StorageMetadata metadata = iterator.next();
                // filter out folders with atmos and filesystem providers
                if (metadata.getType() == StorageType.FOLDER) {
                    continue;
                }
                return metadata;
            }
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
            PayloadBlobBuilder builder = to.blobBuilder(blobName)
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

    static void moveBlob(BlobStore from, BlobStore to,
            String containerNameFrom, String containerNameTo, String blobName)
            throws IOException {
        copyBlob(from, to, containerNameFrom, containerNameTo, blobName);
        from.removeBlob(containerNameFrom, blobName);
    }

    static Properties extractProperties(Properties properties, String prefix) {
        Properties subtree = new Properties();
        properties.entrySet().stream()
                .filter(e -> ((String) e.getKey()).startsWith(prefix))
                .forEach(e -> {
                    String k = ((String) e.getKey()).substring(prefix.length());
                    subtree.put(k, e.getValue());
                });
        return subtree;
    }

    public static void insertAllWithPrefix(Properties dest, String prefix,
                                           Map<String, String> src) {
        src.forEach((k, v) -> dest.put(prefix + k, v));
    }

    /**
     * we don't want to use StorageMetadata.equals directly because that compares
     * fields like URI and ProviderID which we don't care about.
     */
    static boolean equals(StorageMetadata meta1, StorageMetadata meta2) {
        if (meta1 == meta2) {
            return true;
        }
        if (meta1 == null || meta2 == null) {
            return false;
        }
        return Objects.equals(meta1.getCreationDate(), meta2.getCreationDate())
                && Objects.equals(meta1.getETag(), meta2.getETag())
                && Objects.equals(meta1.getLastModified(), meta2.getLastModified())
                && Objects.equals(meta1.getName(), meta2.getName())
                && Objects.equals(meta1.getSize(), meta2.getSize())
                && Objects.equals(meta1.getType(), meta2.getType())
                ;
    }
}
