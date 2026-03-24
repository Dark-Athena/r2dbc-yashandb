package io.r2dbc.yashandb;

import io.r2dbc.spi.Clob;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Adapts an R2DBC {@link Clob} (reactive) to a blocking {@link Reader}
 * suitable for passing to JDBC {@code setClob} / {@code setCharacterStream}.
 *
 * <p>This adapter eagerly materialises the Clob by subscribing synchronously
 * (appropriate only on a boundedElastic thread).</p>
 */
final class R2dbcClobReader extends Reader {

    private final StringReader delegate;

    R2dbcClobReader(Clob clob) {
        // Materialise synchronously
        List<CharSequence> chunks = Flux.from(clob.stream())
                .collectList()
                .block();

        StringBuilder sb = new StringBuilder();
        if (chunks != null) {
            for (CharSequence cs : chunks) {
                sb.append(cs);
            }
        }
        this.delegate = new StringReader(sb.toString());
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        return delegate.read(cbuf, off, len);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
