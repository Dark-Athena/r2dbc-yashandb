package io.r2dbc.yashandb.codec;

import io.r2dbc.yashandb.YashanDbColumnMetadata;
import io.r2dbc.yashandb.YashanDbType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Codec for RAW / BINARY / VARBINARY columns, targeting {@code byte[]}.
 */
public final class ByteArrayCodec implements Codec<byte[]> {

    public static final ByteArrayCodec INSTANCE = new ByteArrayCodec();

    private ByteArrayCodec() {}

    @Override
    public boolean canDecode(YashanDbColumnMetadata metadata, Class<?> targetType) {
        if (!byte[].class.isAssignableFrom(targetType)) return false;
        int jdbcType = ((YashanDbType) metadata.getType()).getJdbcType();
        return jdbcType == Types.BINARY
                || jdbcType == Types.VARBINARY
                || jdbcType == Types.LONGVARBINARY
                || jdbcType == Types.BLOB;
    }

    @Override
    public byte[] decode(ResultSet rs, int jdbcIndex, YashanDbColumnMetadata metadata, Class<? extends byte[]> targetType)
            throws SQLException {
        int jdbcType = ((YashanDbType) metadata.getType()).getJdbcType();
        if (jdbcType == Types.BLOB) {
            java.sql.Blob blob = rs.getBlob(jdbcIndex);
            if (blob == null) return null;
            try {
                return blob.getBytes(1, (int) blob.length());
            } finally {
                blob.free();
            }
        }
        return rs.getBytes(jdbcIndex);
    }
}
