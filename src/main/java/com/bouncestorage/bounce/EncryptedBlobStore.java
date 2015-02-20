/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.spec.KeySpec;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Resource;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import com.google.inject.Inject;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.ContainerAccess;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.domain.Location;
import org.jclouds.io.ContentMetadata;
import org.jclouds.logging.Logger;
import org.jclouds.providers.ProviderMetadata;

public final class EncryptedBlobStore implements BlobStore {

    public static final String BACKEND = "encrypted.backend";
    public static final String KEY = "encrypted.key";
    public static final String SALT = "encrypted.salt";

    @Resource
    private Logger logger = Logger.NULL;

    private BlobStoreContext context;
    private BlobStore delegate;
    private SecretKey secretKey;
    private char[] password;
    private byte[] salt;

    @Inject
    EncryptedBlobStore(BlobStoreContext context, ProviderMetadata providerMetadata) {
        this.context = checkNotNull(context);
        Properties properties = providerMetadata.getDefaultProperties();
        this.password = properties.getProperty(KEY).toCharArray();
        this.salt = properties.getProperty(SALT).getBytes(StandardCharsets.UTF_8);
        initStore(Utils.extractProperties(properties, BACKEND + "."));
    }

    private void initStore(Properties prop) {
        this.delegate = Utils.storeFromProperties(checkNotNull(prop));
        try {
            SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
            // https://github.com/WhisperSystems/TextSecure/issues/184 suggests that
            // 10K is a reasonable order of magnitude for mobile devices
            KeySpec spec = new PBEKeySpec(password, salt, 100000, 128);
            SecretKey tmp = factory.generateSecret(spec);
            secretKey = new SecretKeySpec(tmp.getEncoded(), "AES");
        } catch (GeneralSecurityException e) {
            throw propagate(e);
        }
    }

    BlobStore delegate() {
        return delegate;
    }

    @Override
    public BlobStoreContext getContext() {
        return context;
    }

    @Override
    public BlobBuilder blobBuilder(String s) {
        return delegate.blobBuilder(s);
    }

    @Override
    public Set<? extends Location> listAssignableLocations() {
        return delegate.listAssignableLocations();
    }

    @Override
    public PageSet<? extends StorageMetadata> list() {
        return delegate.list();
    }

    @Override
    public boolean containerExists(String container) {
        return delegate.containerExists(container);
    }

    @Override
    public boolean createContainerInLocation(Location location, String contaniner) {
        return createContainerInLocation(location, contaniner, CreateContainerOptions.NONE);
    }

    @Override
    public boolean createContainerInLocation(Location location, String container, CreateContainerOptions createContainerOptions) {
        return delegate.createContainerInLocation(location, container, createContainerOptions);
    }

    @Override
    public ContainerAccess getContainerAccess(String container) {
        return delegate.getContainerAccess(container);
    }

    @Override
    public void setContainerAccess(String container, ContainerAccess containerAccess) {
        delegate.setContainerAccess(container, containerAccess);
    }

    @Override
    public PageSet<? extends StorageMetadata> list(String container) {
        return delegate.list(container);
    }

    @Override
    public PageSet<? extends StorageMetadata> list(String container, ListContainerOptions listContainerOptions) {
        return delegate.list(container, listContainerOptions);
    }

    @Override
    public void clearContainer(String container) {
        delegate.clearContainer(container);
    }

    @Override
    public void clearContainer(String container, ListContainerOptions listContainerOptions) {
        delegate.clearContainer(container, listContainerOptions);
    }

    @Override
    public void deleteContainer(String container) {
        delegate.deleteContainer(container);
    }

    @Override
    public boolean deleteContainerIfEmpty(String container) {
        return delegate.deleteContainerIfEmpty(container);
    }

    @Override
    public boolean directoryExists(String container, String s) {
        return delegate.directoryExists(container, s);
    }

    @Override
    public void createDirectory(String container, String s) {
        delegate.createDirectory(container, s);
    }

    @Override
    public void deleteDirectory(String container, String s) {
        delegate.deleteDirectory(container, s);
    }

    @Override
    public boolean blobExists(String container, String s) {
        return delegate.blobExists(container, s);
    }

    private Blob cipheredBlob(String container, Blob blob, InputStream payload) {
        ContentMetadata meta = blob.getMetadata().getContentMetadata();
        return blobBuilder(container)
                .name(blob.getMetadata().getName())
                .type(blob.getMetadata().getType())
                .userMetadata(blob.getMetadata().getUserMetadata())
                .payload(payload)
                .contentDisposition(meta.getContentDisposition())
                .contentEncoding(meta.getContentEncoding())
                .contentLanguage(meta.getContentLanguage())
                .contentType(meta.getContentType())
                .expires(meta.getExpires())
                .build();
    }

    private Blob encryptBlob(String container, Blob blob) {
        try {
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            // store the IV in the beginning of the blob
            byte[] iv = cipher.getParameters().getParameterSpec(IvParameterSpec.class).getIV();
            ByteArrayOutputStream ivStream = new ByteArrayOutputStream(32);
            if (iv.length >= 256) {
                throw new UnsupportedOperationException("IV is too long: " + iv.length);
            }
            ivStream.write(iv.length);
            ivStream.write(iv);
            InputStream payload = new SequenceInputStream(
                    new ByteArrayInputStream(ivStream.toByteArray()),
                    new CipherInputStream(blob.getPayload().openStream(), cipher));
            return cipheredBlob(container, blob, payload);
        } catch (IOException | GeneralSecurityException e) {
            throw propagate(e);
        }
    }

    private Blob decryptBlob(String container, Blob blob) {
        try {
            DataInputStream in = new DataInputStream(blob.getPayload().openStream());
            int len = in.read();
            byte[] iv = new byte[len];
            in.readFully(iv);
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(iv));

            return cipheredBlob(container, blob, new CipherInputStream(in, cipher));
        } catch (IOException | GeneralSecurityException e) {
            throw propagate(e);
        }
    }

    @Override
    public String putBlob(String container, Blob blob) {
        return delegate.putBlob(container, encryptBlob(container, blob));
    }

    @Override
    public String putBlob(String container, Blob blob, PutOptions putOptions) {
        return delegate.putBlob(container, encryptBlob(container, blob), putOptions);
    }

    @Override
    public BlobMetadata blobMetadata(String container, String s) {
        return delegate.blobMetadata(container, s);
    }

    @Override
    public Blob getBlob(String container, String s) {
        return decryptBlob(container, delegate.getBlob(container, s));
    }

    @Override
    public Blob getBlob(String container, String s, GetOptions getOptions) {
        return decryptBlob(container, delegate.getBlob(container, s, getOptions));
    }

    @Override
    public void removeBlob(String container, String s) {
        delegate.removeBlob(container, s);
    }

    @Override
    public void removeBlobs(String container, Iterable<String> iterable) {
        delegate.removeBlobs(container, iterable);
    }

    @Override
    public BlobAccess getBlobAccess(String container, String s) {
        return delegate.getBlobAccess(container, s);
    }

    @Override
    public void setBlobAccess(String container, String s, BlobAccess blobAccess) {
        delegate.setBlobAccess(container, s, blobAccess);
    }

    @Override
    public long countBlobs(String container) {
        return delegate.countBlobs(container);
    }

    @Override
    public long countBlobs(String container, ListContainerOptions listContainerOptions) {
        return delegate.countBlobs(container, listContainerOptions);
    }

}
