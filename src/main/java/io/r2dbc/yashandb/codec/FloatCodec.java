package io.r2dbc.yashandb.codec;

import io.r2dbc.yashandb.YashanDbColumnMetadata;
import io.r2dbc.yashandb.YashanDbType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Codec for FLOAT columns, targeting {@link Float}.
 */
public final class FloatCodec implements Codec<Float> {

    public static final FloatCodec INSTANCE = new FloatCodec();

    private FloatCodec() {}

    @Override
    public boolean canDecode(YashanDbColumnMetadata metadata, Class<?> targetType) {
        if (!Float.class.isAssignableFrom(targetType) && targetType != float.class) return false;
        int jdbcType = ((YashanDbType) metadata.getType()).getJdbcType();
        return jdbcType == Types.FLOAT || jdbcType == Types.REAL;
    }

    @Override
    public Float decode(ResultSet rs, int jdbcIndex, YashanDbColumnMetadata metadata, Class<? extends Float> targetType)
            throws SQLException {
        float value = rs.getFloat(jdbcIndex);
        return rs.wasNull() ? null : value;
    }
}
