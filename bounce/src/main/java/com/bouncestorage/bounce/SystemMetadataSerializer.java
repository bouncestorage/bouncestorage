/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;

import org.jclouds.blobstore.domain.MutableStorageMetadata;
import org.jclouds.blobstore.domain.StorageMetadata;

@AutoValue
public abstract class SystemMetadataSerializer<T> {
    public static final String METADATA_PREFIX = "bounce-meta-";
    public static final List<SystemMetadataSerializer> SYSTEM_METADATA = ImmutableList.of(
            SystemMetadataSerializer.create(
                    "etag",
                    StorageMetadata::getETag,
                    Object::toString,
                    String::valueOf,
                    MutableStorageMetadata::setETag),
            SystemMetadataSerializer.create(
                    "mtime",
                    m -> m.getLastModified().toInstant(),
                    Instant::toString,
                    Instant::parse,
                    (m, t) -> m.setLastModified(Date.from(t)))
    );

    public static <T> SystemMetadataSerializer<T> create(String name,
                                                  Function<StorageMetadata, T> getter,
                                                  Function<T, String> serializer,
                                                  Function<String, T> deserializer,
                                                  BiConsumer<MutableStorageMetadata, T> setter) {
        return new AutoValue_SystemMetadataSerializer<T>(
                METADATA_PREFIX + name, getter, serializer, deserializer, setter);
    }

    public String serialize(StorageMetadata meta) {
        T value = getMetadataGetter().apply(meta);
        return getSerializer().apply(value);
    }

    public void deserialize(MutableStorageMetadata meta, String s) {
        T value = getDeserializer().apply(s);
        getMetadataSetter().accept(meta, value);
    }

    public abstract String getName();
    public abstract Function<StorageMetadata, T> getMetadataGetter();
    abstract Function<T, String> getSerializer();
    abstract Function<String, T> getDeserializer();
    abstract BiConsumer<MutableStorageMetadata, T> getMetadataSetter();
}
