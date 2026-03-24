package io.r2dbc.yashandb.codec;

import io.r2dbc.yashandb.YashanDbColumnMetadata;
import io.r2dbc.yashandb.YashanDbType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Codec for BIGINT columns, targeting {@link Long}.
 */
public final class LongCodec implements Codec<Long> {

    public static final LongCodec INSTANCE = new LongCodec();

    private LongCodec() {}

    @Override
    public boolean canDecode(YashanDbColumnMetadata metadata, Class<?> targetType) {
        if (!Long.class.isAssignableFrom(targetType) && targetType != long.class) return false;
        int jdbcType = ((YashanDbType) metadata.getType()).getJdbcType();
        return jdbcType == Types.BIGINT
                || jdbcType == Types.INTEGER
                || jdbcType == Types.SMALLINT
                || jdbcType == Types.TINYINT;
    }

    @Override
    public Long decode(ResultSet rs, int jdbcIndex, YashanDbColumnMetadata metadata, Class<? extends Long> targetType)
            throws SQLException {
        long value = rs.getLong(jdbcIndex);
        return rs.wasNull() ? null : value;
    }
}
