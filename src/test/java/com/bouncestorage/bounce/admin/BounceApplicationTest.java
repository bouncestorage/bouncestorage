/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import com.google.common.reflect.TypeToken;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.MapConfiguration;
import org.jclouds.Context;
import org.jclouds.blobstore.BlobRequestSigner;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.attr.ConsistencyModel;
import org.jclouds.rest.Utils;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public final class BounceApplicationTest {
    private String webConfig;

    @Before
    public void setUp() {
        webConfig = BounceApplicationTest.class.getResource("/bounce.yml").
                toExternalForm();
    }

    final class TestBlobStoreContext implements BlobStoreContext {
        private boolean closed;

        public void close() {
            closed = true;
        }

        public boolean getClosed() {
            return closed;
        }

        @Override
        public BlobRequestSigner getSigner() {
            return null;
        }

        @Override
        public BlobStore getBlobStore() {
            return null;
        }

        @Override
        public ConsistencyModel getConsistencyModel() {
            return null;
        }

        @Override
        public Utils utils() {
            return null;
        }

        @Override
        public TypeToken<?> getBackendType() {
            return null;
        }

        @Override
        public <C extends Context> C unwrap(TypeToken<C> typeToken) throws IllegalArgumentException {
            return null;
        }

        @Override
        public <C extends Context> C unwrap() throws ClassCastException {
            return null;
        }

        @Override
        public <A extends Closeable> A unwrapApi(Class<A> aClass) {
            return null;
        }
    }

    @Test
    public void testBlobStoreContextClose() throws Exception {
        AbstractConfiguration config = new MapConfiguration(new Properties());
        BounceApplication app = new BounceApplication(config);
        TestBlobStoreContext context  = new TestBlobStoreContext();
        app.setBlobStoreContext(context);
        app.run(new String[] {"server", webConfig});
        assertThat(context.getClosed()).isTrue();
    }
}
