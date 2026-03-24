package io.r2dbc.yashandb;

import io.r2dbc.spi.Blob;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

/**
 * Adapts an R2DBC {@link Blob} (reactive) to a blocking {@link InputStream}
 * suitable for passing to JDBC {@code setBlob} / {@code setBinaryStream}.
 *
 * <p>This adapter eagerly materialises the Blob by subscribing to its stream
 * synchronously (appropriate only on a boundedElastic thread).</p>
 */
final class R2dbcBlobInputStream extends InputStream {

    private final byte[] bytes;
    private int position = 0;

    R2dbcBlobInputStream(Blob blob) {
        // Materialise synchronously (we are already on a boundedElastic thread)
        List<ByteBuffer> buffers = Flux.from(blob.stream())
                .collectList()
                .block();

        int total = 0;
        if (buffers != null) {
            for (ByteBuffer buf : buffers) total += buf.remaining();
        }
        this.bytes = new byte[total];
        int offset = 0;
        if (buffers != null) {
            for (ByteBuffer buf : buffers) {
                int len = buf.remaining();
                buf.get(this.bytes, offset, len);
                offset += len;
            }
        }
    }

    @Override
    public int read() throws IOException {
        if (position >= bytes.length) return -1;
        return bytes[position++] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (position >= bytes.length) return -1;
        int available = bytes.length - position;
        int toRead = Math.min(len, available);
        System.arraycopy(bytes, position, b, off, toRead);
        position += toRead;
        return toRead;
    }

    @Override
    public int available() {
        return bytes.length - position;
    }
}
