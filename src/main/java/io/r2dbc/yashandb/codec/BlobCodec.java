package io.r2dbc.yashandb.codec;

import io.r2dbc.spi.Blob;
import io.r2dbc.yashandb.YashanDbColumnMetadata;
import io.r2dbc.yashandb.YashanDbType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Codec for BLOB columns.
 *
 * <ul>
 *   <li>When the target type is {@link Blob}: returns an R2DBC {@link Blob} wrapper.</li>
 *   <li>When the target type is {@link ByteBuffer} or {@link Object}: returns a {@link ByteBuffer}
 *       with the raw bytes.</li>
 * </ul>
 */
public final class BlobCodec implements Codec<Object> {

    public static final BlobCodec INSTANCE = new BlobCodec();

    private BlobCodec() {}

    private static boolean isBlobColumn(YashanDbColumnMetadata metadata) {
        int jdbcType = ((YashanDbType) metadata.getType()).getJdbcType();
        return jdbcType == Types.BLOB
                || jdbcType == Types.BINARY
                || jdbcType == Types.VARBINARY
                || jdbcType == Types.LONGVARBINARY;
    }

    @Override
    public boolean canDecode(YashanDbColumnMetadata metadata, Class<?> targetType) {
        if (!isBlobColumn(metadata)) return false;
        return Blob.class.isAssignableFrom(targetType)
                || ByteBuffer.class.isAssignableFrom(targetType)
                || targetType == Object.class;
    }

    @Override
    public Object decode(ResultSet rs, int jdbcIndex, YashanDbColumnMetadata metadata, Class<?> targetType)
            throws SQLException {
        if (Blob.class.isAssignableFrom(targetType)) {
            byte[] bytes = rs.getBytes(jdbcIndex);
            if (bytes == null) return null;
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            return new Blob() {
                @Override
                public org.reactivestreams.Publisher<ByteBuffer> stream() {
                    return Flux.just(buffer.duplicate());
                }

                @Override
                public org.reactivestreams.Publisher<Void> discard() {
                    return Mono.empty();
                }
            };
        }
        // ByteBuffer or Object: return raw bytes as ByteBuffer
        byte[] bytes = rs.getBytes(jdbcIndex);
        if (bytes == null) return null;
        return ByteBuffer.wrap(bytes);
    }
}
