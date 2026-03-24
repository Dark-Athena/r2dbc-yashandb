package io.r2dbc.yashandb.codec;

import io.r2dbc.yashandb.YashanDbColumnMetadata;
import io.r2dbc.yashandb.YashanDbType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Codec for BOOLEAN / BIT columns.
 */
public final class BooleanCodec implements Codec<Boolean> {

    public static final BooleanCodec INSTANCE = new BooleanCodec();

    private BooleanCodec() {}

    @Override
    public boolean canDecode(YashanDbColumnMetadata metadata, Class<?> targetType) {
        if (!Boolean.class.isAssignableFrom(targetType) && targetType != boolean.class) return false;
        int jdbcType = ((YashanDbType) metadata.getType()).getJdbcType();
        return jdbcType == Types.BOOLEAN || jdbcType == Types.BIT;
    }

    @Override
    public Boolean decode(ResultSet rs, int jdbcIndex, YashanDbColumnMetadata metadata, Class<? extends Boolean> targetType)
            throws SQLException {
        boolean value = rs.getBoolean(jdbcIndex);
        return rs.wasNull() ? null : value;
    }
}
