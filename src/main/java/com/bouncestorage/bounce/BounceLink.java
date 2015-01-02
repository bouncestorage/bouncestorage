/*
 * Copyright 2015 Bounce Storage <info@bouncestorage.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bouncestorage.bounce;

import java.io.*;
import java.net.URI;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

import com.google.common.hash.HashCode;
import com.google.common.io.ByteSource;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.MutableBlobMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.blobstore.domain.internal.MutableBlobMetadataImpl;
import org.jclouds.io.ContentMetadata;
import org.jclouds.io.MutableContentMetadata;
import org.jclouds.io.Payload;
import org.jclouds.io.payloads.ByteSourcePayload;

/**
 * Created by khc on 12/30/14.
 */
public final class BounceLink implements Serializable {

    static final String BOUNCE_LINK = "bounce-link";
    private final MutableBlobMetadata metadata;

    BounceLink(Optional<BlobMetadata> metadata) {
        if (metadata.isPresent()) {
            this.metadata = new MutableBlobMetadataImpl(metadata.get());
        } else {
            this.metadata = new MutableBlobMetadataImpl();
        }
    }

    BlobMetadata getBlobMetadata() {
        return metadata;
    }

    static boolean isLink(BlobMetadata metadata) {
        return metadata.getUserMetadata().containsKey(BOUNCE_LINK);
    }

    static BounceLink fromBlob(Blob b) throws IOException {
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

    Payload toBlobPayload() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(this);
        }
        return new ByteSourcePayload(ByteSource.wrap(bos.toByteArray()));
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.writeObject(metadata.getContainer());
        oos.writeObject(metadata.getPublicUri());
        oos.writeObject(metadata.getCreationDate());
        oos.writeObject(metadata.getETag());
        oos.writeObject(metadata.getLastModified());
        oos.writeObject(metadata.getName());
        //oos.writeObject(metadata.getProviderId());
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
        return (T) ois.readObject();
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        metadata.setContainer(readStr(ois));
        metadata.setPublicUri(readFrom(ois, URI.class));
        metadata.setCreationDate(readFrom(ois, Date.class));
        metadata.setETag(readStr(ois));
        metadata.setLastModified(readFrom(ois, Date.class));
        metadata.setName(readStr(ois));
        metadata.setType(readFrom(ois, StorageType.class));
        metadata.setUri(readFrom(ois, URI.class));
        metadata.setUserMetadata(readFrom(ois, Map.class));
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
