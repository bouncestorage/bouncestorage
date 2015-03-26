/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import static com.google.common.base.Throwables.propagate;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import com.google.common.io.ByteSource;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.MutableBlobMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.blobstore.domain.internal.MutableBlobMetadataImpl;
import org.jclouds.io.ContentMetadata;
import org.jclouds.io.MutableContentMetadata;
import org.jclouds.io.Payload;
import org.jclouds.io.payloads.ByteSourcePayload;

public final class BounceLink implements Serializable {

    private static final long serialVersionUID = 0;
    private static final String BOUNCE_LINK = "bounce-link";
    private static final Map<String, String> BOUNCE_ATTR = ImmutableMap.of(
            BOUNCE_LINK, ""
    );
    private MutableBlobMetadata metadata;

    public BounceLink(Optional<BlobMetadata> metadata) {
        if (metadata.isPresent()) {
            this.metadata = new MutableBlobMetadataImpl(metadata.get());
        } else {
            this.metadata = new MutableBlobMetadataImpl();
        }
    }

    public BlobMetadata getBlobMetadata() {
        return metadata;
    }

    public static boolean isLink(BlobMetadata metadata) {
        return metadata.getUserMetadata().containsKey(BOUNCE_LINK);
    }

    public static BounceLink fromBlob(Blob b) throws IOException {
        try (InputStream is = b.getPayload().openStream();
            ObjectInputStream ois = new ObjectInputStream(is)) {
            Object obj = ois.readObject();
            if (obj instanceof BounceLink) {
                return (BounceLink) obj;
            }
            return null;
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    public Blob toBlob(BlobStore store) {
        return store.blobBuilder(metadata.getName())
                .payload(toBlobPayload())
                .userMetadata(BounceLink.BOUNCE_ATTR)
                .build();
    }

    private Payload toBlobPayload() {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                oos.writeObject(this);
            }
            return new ByteSourcePayload(ByteSource.wrap(bos.toByteArray()));
        } catch (IOException e) {
            throw propagate(e);
        }
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.writeObject(metadata.getContainer());
        oos.writeObject(metadata.getPublicUri());
        oos.writeObject(metadata.getCreationDate());
        oos.writeObject(metadata.getETag());
        oos.writeObject(metadata.getLastModified());
        oos.writeObject(metadata.getName());
        //oos.writeObject(metadata.getProviderId());
        oos.writeLong(metadata.getSize());
        oos.writeObject(metadata.getType());
        oos.writeObject(metadata.getUri());
        oos.writeObject(metadata.getUserMetadata());
        ContentMetadata cmeta = metadata.getContentMetadata();
        oos.writeObject(cmeta.getContentDisposition());
        oos.writeObject(cmeta.getContentEncoding());
        oos.writeObject(cmeta.getContentLanguage());
        oos.writeLong(cmeta.getContentLength());
        byte[] md5 = cmeta.getContentMD5AsHashCode().asBytes();
        oos.writeInt(md5.length);
        oos.write(md5);
        oos.writeObject(cmeta.getContentType());
        oos.writeObject(cmeta.getExpires());
    }

    private static String readStr(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        return readFrom(ois, String.class);
    }

    private static <T> T readFrom(ObjectInputStream ois, Class<T> c) throws IOException, ClassNotFoundException {
        @SuppressWarnings("unchecked")
        T object = (T) ois.readObject();
        return object;
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        metadata = new MutableBlobMetadataImpl();
        metadata.setContainer(readStr(ois));
        metadata.setPublicUri(readFrom(ois, URI.class));
        metadata.setCreationDate(readFrom(ois, Date.class));
        metadata.setETag(readStr(ois));
        metadata.setLastModified(readFrom(ois, Date.class));
        metadata.setName(readStr(ois));
        metadata.setSize(ois.readLong());
        metadata.setType(readFrom(ois, StorageType.class));
        metadata.setUri(readFrom(ois, URI.class));
        @SuppressWarnings("unchecked")
        Map<String, String> userMetadata =
                (Map<String, String>) readFrom(ois, Map.class);
        metadata.setUserMetadata(userMetadata);
        MutableContentMetadata cmeta = metadata.getContentMetadata();
        cmeta.setContentDisposition(readStr(ois));
        cmeta.setContentEncoding(readStr(ois));
        cmeta.setContentLanguage(readStr(ois));
        cmeta.setContentLength(ois.readLong());
        int md5Length = ois.readInt();
        byte[] md5 = new byte[md5Length];
        ois.readFully(md5);
        cmeta.setContentMD5(HashCode.fromBytes(md5));
        cmeta.setContentType(readStr(ois));
        cmeta.setExpires(readFrom(ois, Date.class));
    }
}
