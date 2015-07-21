/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import static com.google.common.base.Throwables.propagate;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;

import com.bouncestorage.bounce.admin.BouncePolicy;
import com.bouncestorage.bounce.admin.policy.WriteBackPolicy;
import com.bouncestorage.bounce.utils.BlobStoreByteSource;
import com.google.common.collect.AbstractIterator;
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
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.io.ContentMetadata;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;

public final class Utils {
    public static final int WAIT_SLEEP_MS = 10;
    public static final int WAIT_TIMEOUT_MS = 30 * 1000;
    private static final PutOptions MULTIPART_PUT = new PutOptions().multipart(true);

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
                WriteBackPolicy.LOG_MARKER_SUFFIX);
        return BouncePolicy.BounceResult.LINK;
    }

    public static void waitUntil(Callable<Boolean> test) throws Exception {
        waitUntil(WAIT_SLEEP_MS, WAIT_TIMEOUT_MS, test);
    }

    public static void waitUntil(long waitMs, long timeoutMs, Callable<Boolean> test) throws Exception {
        Instant timeStarted = Instant.now();
        while (!test.call()) {
            Thread.sleep(waitMs);
            if (Instant.now().minusMillis(timeoutMs).isAfter(timeStarted)) {
                throw new TimeoutException("Application took more than 30 seconds to start");
            }
        }
    }

    public static String createRandomContainerName() {
        return "bounce-" + new Random().nextInt(Integer.MAX_VALUE);
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
            extends AbstractIterator<StorageMetadata> {
        private final BlobStore blobStore;
        private final String containerName;
        private final ListContainerOptions options;
        private Iterator<? extends StorageMetadata> iterator;
        private String marker;

        CrawlBlobStoreIterator(BlobStore blobStore, String containerName,
                ListContainerOptions options) {
            this.blobStore = Objects.requireNonNull(blobStore);
            this.containerName = Objects.requireNonNull(containerName);
            this.options = Objects.requireNonNull(options);
            if (options.getDelimiter() == null && options.getDir() == null) {
                this.options.recursive();
            }
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
        protected StorageMetadata computeNext() {
            while (true) {
                if (!iterator.hasNext()) {
                    if (marker == null) {
                        return endOfData();
                    }
                    advance();
                    continue;
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

    public static Blob copyBlob(BlobStore from, BlobStore to, String containerNameTo, Blob blobFrom, InputStream is)
        throws IOException {
        if (blobFrom == null || BounceLink.isLink(blobFrom.getMetadata())) {
            return null;
        }
        ContentMetadata metadata = blobFrom.getMetadata().getContentMetadata();
        PayloadBlobBuilder builder = to.blobBuilder(blobFrom.getMetadata().getName())
                .userMetadata(blobFrom.getMetadata().getUserMetadata())
                .payload(is);

        copyToBlobBuilder(metadata, builder);

        if (isSwiftBlobStore(to)) {
            copySwiftBlob(from, to, containerNameTo, builder.build());
        } else {
            to.putBlob(containerNameTo, builder.build(), MULTIPART_PUT);
        }
        return blobFrom;
    }

    private static void copyToBlobBuilder(ContentMetadata metadata, PayloadBlobBuilder builder) {
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
        } else {
            throw new UnsupportedOperationException("S3 doesn't allow NULL content length");
        }

        String contentType = metadata.getContentType();
        if (contentType != null) {
            builder.contentType(metadata.getContentType());
        }

        Date expires = metadata.getExpires();
        if (expires != null) {
            builder.expires(expires);
        }
    }

    private static boolean isSwiftBlobStore(BlobStore blobStore) {
        return blobStore.getContext().unwrap().getId().equals("openstack-swift");
    }

    private static void copySwiftBlob(BlobStore from, BlobStore to, String containerNameTo, Blob blob) {
        // TODO: swift object semantic changes if we do multipart upload,
        // if both sides are swift, we may want to just copy the individual parts
        PutOptions options = PutOptions.NONE;
        if (blob.getMetadata().getContentMetadata().getContentLength() > to.getMaximumMultipartPartSize()) {
            options = MULTIPART_PUT;
        }
        to.putBlob(containerNameTo, blob, options);
    }

    public static Blob copyBlob(BlobStore from, BlobStore to, String containerNameTo, Blob blobFrom)
            throws IOException {
        if (blobFrom == null || BounceLink.isLink(blobFrom.getMetadata())) {
            return null;
        }

        ContentMetadata metadata = blobFrom.getMetadata().getContentMetadata();
        PayloadBlobBuilder builder = to.blobBuilder(blobFrom.getMetadata().getName())
                .userMetadata(blobFrom.getMetadata().getUserMetadata())
                .payload(new BlobStoreByteSource(from, blobFrom, blobFrom.getMetadata().getSize()));

        copyToBlobBuilder(metadata, builder);

        if (isSwiftBlobStore(to)) {
            copySwiftBlob(from, to, containerNameTo, builder.build());
        } else {
            PutOptions options = PutOptions.NONE;
            if (blobFrom.getMetadata().getSize() >= to.getMinimumMultipartPartSize()) {
                options = MULTIPART_PUT;
            }
            to.putBlob(containerNameTo, builder.build(), options);
        }
        return blobFrom;
    }


    // TODO: eventually this should support parallel copies, cancellation, and
    // multi-part uploads
    public static Blob copyBlob(BlobStore from, BlobStore to,
            String containerNameFrom, String containerNameTo, String blobName)
            throws IOException {
        Blob blobFrom = from.getBlob(containerNameFrom, blobName);
        if (blobFrom == null) {
            return null;
        }

        return copyBlob(from, to, containerNameTo, blobFrom);
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

    public static String trimETag(@Nullable String eTag) {
        if (eTag == null) {
            return null;
        }
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

    public static boolean eTagsEqual(@Nullable String eTag1, @Nullable String eTag2) {
        return Objects.equals(trimETag(eTag1), trimETag(eTag2));
    }

    public static StorageMetadata getNextOrNull(PeekingIterator<StorageMetadata> iterator) {
        return iterator.hasNext() ? iterator.next() : null;
    }
}
