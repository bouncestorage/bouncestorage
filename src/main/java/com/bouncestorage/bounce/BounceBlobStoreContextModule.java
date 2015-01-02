package com.bouncestorage.bounce;

import com.google.inject.AbstractModule;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.attr.ConsistencyModel;
import org.jclouds.blobstore.config.BlobStoreObjectModule;

/**
 * Created by khc on 1/1/15.
 */
public class BounceBlobStoreContextModule extends AbstractModule {
    @Override
    protected void configure() {
        install(new BlobStoreObjectModule());
        bind(BlobStore.class).to(BounceBlobStore.class);
        bind(ConsistencyModel.class).toInstance(ConsistencyModel.EVENTUAL);
    }
}
