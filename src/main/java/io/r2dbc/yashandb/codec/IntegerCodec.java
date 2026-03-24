package io.r2dbc.yashandb.codec;

import io.r2dbc.yashandb.YashanDbColumnMetadata;
import io.r2dbc.yashandb.YashanDbType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Codec for INTEGER / SMALLINT / TINYINT columns, targeting {@link Integer}.
 */
public final class IntegerCodec implements Codec<Integer> {

    public static final IntegerCodec INSTANCE = new IntegerCodec();

    private IntegerCodec() {}

    @Override
    public boolean canDecode(YashanDbColumnMetadata metadata, Class<?> targetType) {
        if (!Integer.class.isAssignableFrom(targetType) && targetType != int.class) return false;
        int jdbcType = ((YashanDbType) metadata.getType()).getJdbcType();
        return jdbcType == Types.INTEGER
                || jdbcType == Types.SMALLINT
                || jdbcType == Types.TINYINT;
    }

    @Override
    public Integer decode(ResultSet rs, int jdbcIndex, YashanDbColumnMetadata metadata, Class<? extends Integer> targetType)
            throws SQLException {
        int value = rs.getInt(jdbcIndex);
        return rs.wasNull() ? null : value;
    }
}
