/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import com.google.common.base.Optional;
import com.google.common.io.ByteSource;

public final class TestUtils {
    private TestUtils() {
        throw new AssertionError("intentionally unimplemented");
    }

    public static ByteSource randomByteSource() {
        return randomByteSource(0);
    }

    public static ByteSource randomByteSource(long seed) {
        return new RandomByteSource(seed);
    }

    private static class RandomByteSource extends ByteSource {
        private final long seed;

        RandomByteSource(long seed) {
            this.seed = seed;
        }

        @Override
        public InputStream openStream() {
            return new RandomInputStream(seed);
        }

        @Override
        public Optional<Long> sizeIfKnown() {
            return Optional.of(Long.MAX_VALUE);
        }
    }

    private static class RandomInputStream extends InputStream {
        private final Random random;
        private boolean closed;

        RandomInputStream(long seed) {
            this.random = new Random(seed);
        }

        @Override
        public synchronized int read() throws IOException {
            if (closed) {
                throw new IOException("Stream already closed");
            }
            return (byte) random.nextInt();
        }

        @Override
        public synchronized int read(byte[] b) throws IOException {
            return read(b, 0, b.length);
        }

        @Override
        public synchronized int read(byte[] b, int off, int len)
                throws IOException {
            for (int i = 0; i < len; ++i) {
                b[off + i] = (byte) read();
            }
            return len;
        }

        @Override
        public void close() throws IOException {
            super.close();
            closed = true;
        }
    }
}
