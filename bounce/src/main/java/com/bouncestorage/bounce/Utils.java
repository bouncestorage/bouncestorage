/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import static com.google.common.base.Throwables.propagate;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import com.bouncestorage.bounce.admin.BouncePolicy;
import com.bouncestorage.bounce.admin.policy.MarkerPolicy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.PeekingIterator;
import com.google.common.hash.HashCode;
import com.google.inject.Module;

import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder.PayloadBlobBuilder;
import org.jclouds.blobstore.domain.BlobMetadata;
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
        if (provider == null) {
            return null;
        }
        ContextBuilder builder = ContextBuilder
                .newBuilder(provider)
                .modules(ImmutableList.<Module>of(new SLF4JLoggingModule()))
                .overrides(properties);
        BlobStoreContext context = builder.build(BlobStoreContext.class);
        return context.getBlobStore();
    }

    public static BouncePolicy.BounceResult copyBlobAndCreateBounceLink(
            BlobStore src, BlobStore dest, String containerName, String blobName) {
        try {
            Blob blobFrom = copyBlob(src, dest, containerName, containerName, blobName);
            if (blobFrom != null) {
                createBounceLink(src, blobFrom.getMetadata());
            }
        } catch (IOException e) {
            propagate(e);
        }
        return BouncePolicy.BounceResult.MOVE;
    }

    public static BouncePolicy.BounceResult createBounceLink(BlobStore blobStore, BlobMetadata blobMetadata) {
        BounceLink link = new BounceLink(Optional.of(blobMetadata));
        blobStore.putBlob(blobMetadata.getContainer(), link.toBlob(blobStore));
        blobStore.removeBlob(blobMetadata.getContainer(), blobMetadata.getName() +
                MarkerPolicy.LOG_MARKER_SUFFIX);
        return BouncePolicy.BounceResult.LINK;
    }

    private static class CrawlBlobStoreIterable
            implements Iterable<StorageMetadata> {
        private final BlobStore blobStore;
        private final String containerName;
        private final ListContainerOptions options;

        CrawlBlobStoreIterable(BlobStore blobStore, String containerName,
                ListContainerOptions options) {
            this.blobStore = Objects.requireNonNull(blobStore);
            this.containerName = Objects.requireNonNull(containerName);
            this.options = Objects.requireNonNull(options);
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
            this.blobStore = Objects.requireNonNull(blobStore);
            this.containerName = Objects.requireNonNull(containerName);
            this.options = Objects.requireNonNull(options).recursive();
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
            if (iterator.hasNext()) {
                return true;
            }
            // Presence of a marker does not guarantee that subsequent results; we must advance to determine this.
            if (marker != null) {
                advance();
            }
            return iterator.hasNext();
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
    public static Blob copyBlob(BlobStore from, BlobStore to,
            String containerNameFrom, String containerNameTo, String blobName)
            throws IOException {
        Blob blobFrom = from.getBlob(containerNameFrom, blobName);
        if (blobFrom == null || BounceLink.isLink(blobFrom.getMetadata())) {
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
    public static boolean equals(StorageMetadata meta1, StorageMetadata meta2) {
        if (meta1 == meta2) {
            return true;
        }
        if (meta1 == null || meta2 == null) {
            return false;
        }
        return Objects.equals(meta1.getCreationDate(), meta2.getCreationDate())
                && eTagsEqual(meta1.getETag(), meta2.getETag())
                && Objects.equals(meta1.getLastModified(), meta2.getLastModified())
                && Objects.equals(meta1.getName(), meta2.getName())
                && Objects.equals(meta1.getSize(), meta2.getSize())
                && Objects.equals(meta1.getType(), meta2.getType())
                ;
    }

    public static boolean equalsOtherThanTime(StorageMetadata meta1, StorageMetadata meta2) {
        if (meta1 == meta2) {
            return true;
        }
        if (meta1 == null || meta2 == null) {
            return false;
        }
        return eTagsEqual(meta1.getETag(), meta2.getETag())
                && Objects.equals(meta1.getName(), meta2.getName())
                && Objects.equals(meta1.getSize(), meta2.getSize())
                && Objects.equals(meta1.getType(), meta2.getType())
                ;
    }

    public static String trimETag(String eTag) {
        int begin = 0;
        int end = eTag.length();
        if (eTag.startsWith("\"")) {
            begin = 1;
        }
        if (eTag.endsWith("\"")) {
            end = eTag.length() - 1;
        }
        return eTag.substring(begin, end);
    }

    public static boolean eTagsEqual(String eTag1, String eTag2) {
        return trimETag(eTag1).equals(trimETag(eTag2));
    }

    public static StorageMetadata getNextOrNull(PeekingIterator<StorageMetadata> iterator) {
        return iterator.hasNext() ? iterator.next() : null;
    }
}
