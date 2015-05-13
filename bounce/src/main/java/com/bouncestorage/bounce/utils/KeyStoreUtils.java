/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.utils;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Throwables.propagate;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Enumeration;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.lang3.tuple.Pair;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KeyStoreUtils {
    private final KeyStore keyStore;
    private final String path;
    private final char[] password;
    private Logger logger = LoggerFactory.getLogger(getClass());

    private KeyStoreUtils(KeyStore keyStore, String path, char[] password) {
        this.keyStore = requireNonNull(keyStore);
        this.path = path;
        this.password = requireNonNull(password);
    }

    private static InputStream getInputStream(String path) {
        try {
            return new FileInputStream(path);
        } catch (FileNotFoundException e) {
            return null;
        }
    }

    @VisibleForTesting
    public static KeyStoreUtils getTestingKeyStore() throws GeneralSecurityException, IOException {
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        char[] password = new char[0];
        ks.load(null, password);
        return new KeyStoreUtils(ks, null, password);
    }

    public static KeyStoreUtils getKeyStore(String path, String password)
            throws GeneralSecurityException, IOException {
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        InputStream in = getInputStream(path);
        char[] passChars = password.toCharArray();
        ks.load(in, passChars);
        if (in == null) {
            try (FileOutputStream out = new FileOutputStream(path)) {
                ks.store(out, passChars);
            }
        }
        return new KeyStoreUtils(ks, path, passChars);
    }

    private Pair<Key, X509Certificate> generateKey(String name)
            throws GeneralSecurityException, OperatorCreationException {
        logger.debug("generating self-signed cert for {}", name);
        BouncyCastleProvider provider = new BouncyCastleProvider();
        Security.addProvider(provider);
        KeyPairGenerator kpGen = KeyPairGenerator.getInstance("RSA", provider);
        kpGen.initialize(1024, new SecureRandom());
        KeyPair pair = kpGen.generateKeyPair();
        X500NameBuilder builder = new X500NameBuilder(BCStyle.INSTANCE);
        builder.addRDN(BCStyle.OU, "None");
        builder.addRDN(BCStyle.O, "None");
        builder.addRDN(BCStyle.CN, name);
        Instant now = Instant.now();
        Date notBefore = Date.from(now);
        Date notAfter = Date.from(now.plus(365, ChronoUnit.DAYS));
        BigInteger serial = BigInteger.valueOf(now.getEpochSecond());
        X509v3CertificateBuilder certGen = new JcaX509v3CertificateBuilder(builder.build(), serial, notBefore, notAfter,
                builder.build(), pair.getPublic());
        ContentSigner sigGen = new JcaContentSignerBuilder("SHA256WithRSAEncryption")
                .setProvider(provider)
                .build(pair.getPrivate());
        X509Certificate cert = new JcaX509CertificateConverter()
                .setProvider(provider)
                .getCertificate(certGen.build(sigGen));
        return Pair.of(pair.getPrivate(), cert);
    }

    public String exportToPem(X509Certificate certificate) {
        StringWriter writer = new StringWriter();
        try (JcaPEMWriter pem = new JcaPEMWriter(writer)) {
            pem.writeObject(certificate);
            pem.flush();
            return writer.getBuffer().toString();
        } catch (IOException e) {
            throw propagate(e);
        }
    }

    public X509Certificate ensureCertificate(String name)
            throws GeneralSecurityException, IOException, OperatorCreationException {
        Enumeration<String> aliases = keyStore.aliases();
        KeyStore.PasswordProtection protection = new KeyStore.PasswordProtection(password);
        String wantCN = "CN=" + name + ",";
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            logger.debug("found {} in keystore", alias);
            KeyStore.Entry entry = keyStore.getEntry(alias, protection);
            if (!(entry instanceof KeyStore.PrivateKeyEntry)) {
                logger.debug("alias is {}", entry.getClass());
                continue;
            }
            Certificate cert = ((KeyStore.PrivateKeyEntry) entry).getCertificate();
            if (cert instanceof X509Certificate) {
                X509Certificate x509 = (X509Certificate) cert;
                if (x509.getSubjectX500Principal().getName().indexOf(wantCN) != -1) {
                    logger.debug("cert for {} already exists", name);
                    return x509;
                } else {
                    logger.debug("{}", x509.getSubjectX500Principal().getName());
                }
            }
        }

        Pair<Key, X509Certificate> entry = generateKey(name);
        keyStore.setKeyEntry(name, entry.getLeft(), password, new X509Certificate[]{entry.getRight()});
        if (path != null) {
            keyStore.store(new FileOutputStream(path), password);
        }
        return entry.getRight();
    }
}
